//! Procedural macros for Stage.
//!
//! * `#[stage::actor]` on a **struct** generates `spawn`/`spawn_on`/`spawn_with`.
//! * `#[stage::actor]` on an **impl block** lowers each async `self`-method into
//!   an `ActorContext`-based body and generates the corresponding
//!   `ActorRef<T>::method` that schedules it.
//! * `#[stage::actor_fn]` turns a free `async fn` into a schedulable helper
//!   invoked as `name(&actor_ref, ..)`. Two forms, by the first parameter:
//!   * **with `ctx`** — `async fn helper(ctx: ActorContext<'_, A>, ..)` reads
//!     actor state. May take only `ctx`, and may be generic over the actor type
//!     (`helper<A: Trait>(ctx: ActorContext<'_, A>)`) for reuse across actors.
//!   * **without `ctx`** — `async fn work(..)` (no `ActorContext`) runs *on* an
//!     actor (its token + reentrancy) but never reads actor state. It is made
//!     generic over the actor type, so `work(&any_actor, ..)` runs it on any
//!     actor. (This is the named-helper equivalent of `stage::run_on`.)

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use std::collections::HashSet;
use syn::visit_mut::VisitMut;
use syn::{
    parse_macro_input, parse_quote, Expr, FnArg, GenericArgument, GenericParam, Ident, ImplItem,
    ImplItemFn, Item, ItemFn, ItemImpl, ItemStruct, Pat, PathArguments, ReturnType, Type,
};

#[proc_macro_attribute]
pub fn actor(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let item = parse_macro_input!(item as Item);
    match item {
        Item::Struct(s) => actor_struct(s),
        Item::Impl(i) => actor_impl(i),
        other => syn::Error::new_spanned(
            other,
            "#[stage::actor] may only be applied to a struct or an impl block",
        )
        .to_compile_error()
        .into(),
    }
}

fn actor_struct(s: ItemStruct) -> TokenStream {
    let name = &s.ident;
    if !s.generics.params.is_empty() {
        return syn::Error::new_spanned(
            &s.generics,
            "#[stage::actor] does not support generic actors in this prototype",
        )
        .to_compile_error()
        .into();
    }
    quote! {
        #s

        impl #name {
            /// Spawn this actor on the default executor (requires `Default`).
            pub fn spawn() -> ::stage::ActorRef<#name>
            where #name: ::core::default::Default {
                ::stage::ActorRef::<#name>::__spawn_default(&::stage::default_executor())
            }
            /// Spawn this actor on a specific executor (requires `Default`).
            pub fn spawn_on(__ex: &::stage::Executor) -> ::stage::ActorRef<#name>
            where #name: ::core::default::Default {
                ::stage::ActorRef::<#name>::__spawn_default(__ex)
            }
            /// Spawn this actor with an explicit initial state on the default executor.
            pub fn spawn_with(__state: #name) -> ::stage::ActorRef<#name> {
                ::stage::ActorRef::<#name>::__spawn_with(&::stage::default_executor(), __state)
            }
            /// Spawn this actor with an explicit initial state on a specific executor.
            pub fn spawn_with_on(__ex: &::stage::Executor, __state: #name) -> ::stage::ActorRef<#name> {
                ::stage::ActorRef::<#name>::__spawn_with(__ex, __state)
            }
        }
    }
    .into()
}

fn actor_impl(input: ItemImpl) -> TokenStream {
    let self_ty = &input.self_ty;
    let actor_ident = match type_ident(self_ty) {
        Some(id) => id,
        None => {
            return syn::Error::new_spanned(
                self_ty,
                "#[stage::actor] impl target must be a simple type name",
            )
            .to_compile_error()
            .into()
        }
    };
    // We cannot write an inherent `impl ActorRef<Counter>` (ActorRef is foreign,
    // E0116). Instead we generate an extension trait and impl it for
    // `ActorRef<Counter>`. The trait is defined in the same module as the actor,
    // so it is automatically in scope for `counter.method()` calls there.
    let trait_ident = format_ident!("__StageMethods_{}", actor_ident);

    // Names of async self-methods, so intra-actor `self.method().await` calls
    // can be lowered to inline calls that continue in the same continuation.
    let actor_methods: HashSet<String> = input
        .items
        .iter()
        .filter_map(|item| match item {
            ImplItem::Fn(f) if is_actor_method(f) => Some(f.sig.ident.to_string()),
            _ => None,
        })
        .collect();

    let mut lowered = Vec::new();
    let mut trait_sigs = Vec::new();
    let mut impl_methods = Vec::new();
    let mut passthrough = Vec::new();

    for item in &input.items {
        match item {
            ImplItem::Fn(f) if is_actor_method(f) => {
                let (low, sig, m) = lower_method(self_ty, f, &actor_methods);
                lowered.push(low);
                trait_sigs.push(sig);
                impl_methods.push(m);
            }
            other => passthrough.push(other.clone()),
        }
    }

    quote! {
        impl #self_ty {
            #(#lowered)*
            #(#passthrough)*
        }

        /// Generated extension trait carrying this actor's message-sending
        /// methods. In scope automatically within the defining module.
        #[allow(non_camel_case_types)]
        pub trait #trait_ident {
            #(#trait_sigs)*
        }

        impl #trait_ident for ::stage::ActorRef<#self_ty> {
            #(#impl_methods)*
        }
    }
    .into()
}

/// An "actor method" is an async method whose first parameter is a receiver.
fn is_actor_method(f: &ImplItemFn) -> bool {
    f.sig.asyncness.is_some() && matches!(f.sig.inputs.first(), Some(FnArg::Receiver(_)))
}

fn lower_method(
    self_ty: &Type,
    f: &ImplItemFn,
    actor_methods: &HashSet<String>,
) -> (
    proc_macro2::TokenStream,
    proc_macro2::TokenStream,
    proc_macro2::TokenStream,
) {
    let name = &f.sig.ident;
    let lowered_name = format_ident!("__stage_method_{}", name);
    let ctx_ident = format_ident!("__stage_ctx");

    let (typed, idents) = split_args(f.sig.inputs.iter().skip(1));

    let ret = &f.sig.output;
    let ret_ty = match ret {
        ReturnType::Default => quote! { () },
        ReturnType::Type(_, t) => quote! { #t },
    };

    // Rewrite the body: `self.method(..)` for an async actor method becomes an
    // inline call to the lowered associated fn (continues in this continuation);
    // every other `self` becomes the context handle.
    let mut body = f.block.clone();
    RewriteBody {
        ctx: ctx_ident.clone(),
        self_ty: self_ty.clone(),
        actor_methods,
    }
    .visit_block_mut(&mut body);

    let lowered = quote! {
        #[allow(unused, non_snake_case, clippy::all)]
        async fn #lowered_name(
            mut #ctx_ident: ::stage::ActorContext<'_, #self_ty>,
            #(#typed),*
        ) #ret #body
    };

    let trait_sig = quote! {
        fn #name(&self, #(#typed),*) -> ::stage::JoinHandle<#ret_ty>;
    };

    let impl_method = quote! {
        fn #name(&self, #(#typed),*) -> ::stage::JoinHandle<#ret_ty> {
            let __cell = ::stage::ActorRef::__cell(self);
            ::stage::__private::spawn_method(__cell, async move {
                <#self_ty>::#lowered_name(::stage::__private::__ctx(), #(#idents),*).await
            })
        }
    };

    (lowered, trait_sig, impl_method)
}

#[proc_macro_attribute]
pub fn actor_fn(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let f = parse_macro_input!(item as ItemFn);

    if f.sig.asyncness.is_none() {
        return syn::Error::new_spanned(&f.sig.ident, "#[stage::actor_fn] must be `async`")
            .to_compile_error()
            .into();
    }

    // Two forms, chosen by the first parameter:
    //   * `ctx: ActorContext<'_, A>` first  -> the helper reads actor state.
    //   * no such first parameter            -> the helper runs *on* an actor but
    //                                           does not read its state; it is
    //                                           generic over the actor type.
    let has_ctx = matches!(
        f.sig.inputs.first(),
        Some(FnArg::Typed(pt)) if extract_actor_ty(&pt.ty).is_some()
    );

    if has_ctx {
        actor_fn_with_ctx(f)
    } else {
        actor_fn_no_ctx(f)
    }
}

/// `#[stage::actor_fn]` where the first parameter is `ctx: ActorContext<'_, A>`.
fn actor_fn_with_ctx(f: ItemFn) -> TokenStream {
    let first = match f.sig.inputs.first() {
        Some(FnArg::Typed(pt)) => pt.clone(),
        _ => unreachable!("checked by caller"),
    };
    let actor_ty = extract_actor_ty(&first.ty).expect("checked by caller");

    // The context is dereferenced mutably in the body, so the binding must be
    // `mut` regardless of how the user wrote it.
    let ctx_ident = match &*first.pat {
        Pat::Ident(pi) => pi.ident.clone(),
        _ => {
            return syn::Error::new_spanned(
                &first.pat,
                "the ActorContext parameter must be a simple identifier",
            )
            .to_compile_error()
            .into()
        }
    };
    let ctx_ty = &first.ty;
    let first_param = quote! { mut #ctx_ident: #ctx_ty };

    let vis = &f.vis;
    let name = &f.sig.ident;
    let lowered_name = format_ident!("__stage_fn_{}", name);
    let body = &f.block;
    let asyncness = &f.sig.asyncness;
    let ret = &f.sig.output;
    let ret_ty = return_type(ret);

    let (typed, idents) = split_args(f.sig.inputs.iter().skip(1));

    // Propagate the function's own generics + where-clause so a helper can be
    // generic over the actor type, e.g. `async fn helper<A: SomeTrait>(ctx:
    // ActorContext<'_, A>)`, and be invoked from multiple distinct actor types.
    let (impl_generics, _ty_generics, where_clause) = f.sig.generics.split_for_impl();

    quote! {
        #[allow(unused, non_snake_case, clippy::all)]
        #asyncness fn #lowered_name #impl_generics (#first_param, #(#typed),*) #ret
        #where_clause
        #body

        #vis fn #name #impl_generics (
            __actor: &::stage::ActorRef<#actor_ty>,
            #(#typed),*
        ) -> ::stage::JoinHandle<#ret_ty>
        #where_clause
        {
            let __cell = ::stage::ActorRef::__cell(__actor);
            ::stage::__private::spawn_method(__cell, async move {
                #lowered_name(::stage::__private::__ctx::<#actor_ty>(), #(#idents),*).await
            })
        }
    }
    .into()
}

/// `#[stage::actor_fn]` with no `ctx` parameter: the helper runs on an actor
/// (its token + reentrancy) but never reads or declares actor state. It is made
/// generic over the actor type, so it can be invoked on any actor:
/// `work(&some_actor, ..).await`.
fn actor_fn_no_ctx(f: ItemFn) -> TokenStream {
    let vis = &f.vis;
    let name = &f.sig.ident;
    let lowered_name = format_ident!("__stage_fn_{}", name);
    let body = &f.block;
    let asyncness = &f.sig.asyncness;
    let ret = &f.sig.output;
    let ret_ty = return_type(ret);

    // Every parameter is an ordinary argument (no ctx to skip).
    let (typed, idents) = split_args(f.sig.inputs.iter());

    // The lowered body keeps the user's own generics (it never names the actor).
    let (impl_generics, _ty_generics, where_clause) = f.sig.generics.split_for_impl();

    // The public wrapper additionally takes a fresh actor type parameter and an
    // `&ActorRef` to schedule on. Insert it after any leading lifetimes.
    let actor_param = format_ident!("__StageActor");
    let mut wrapper_generics = f.sig.generics.clone();
    let new_param: GenericParam = parse_quote!(#actor_param: ::core::marker::Send + 'static);
    let insert_at = wrapper_generics
        .params
        .iter()
        .take_while(|p| matches!(p, GenericParam::Lifetime(_)))
        .count();
    wrapper_generics.params.insert(insert_at, new_param);
    let (wrapper_impl, _w_ty, wrapper_where) = wrapper_generics.split_for_impl();

    quote! {
        #[allow(unused, non_snake_case, clippy::all)]
        #asyncness fn #lowered_name #impl_generics (#(#typed),*) #ret
        #where_clause
        #body

        #vis fn #name #wrapper_impl (
            __actor: &::stage::ActorRef<#actor_param>,
            #(#typed),*
        ) -> ::stage::JoinHandle<#ret_ty>
        #wrapper_where
        {
            let __cell = ::stage::ActorRef::__cell(__actor);
            ::stage::__private::spawn_method(__cell, async move {
                #lowered_name(#(#idents),*).await
            })
        }
    }
    .into()
}

fn return_type(ret: &ReturnType) -> proc_macro2::TokenStream {
    match ret {
        ReturnType::Default => quote! { () },
        ReturnType::Type(_, t) => quote! { #t },
    }
}

/// Split typed args into `(param tokens, call-site idents)`.
fn split_args<'a>(
    args: impl Iterator<Item = &'a FnArg>,
) -> (Vec<proc_macro2::TokenStream>, Vec<proc_macro2::TokenStream>) {
    let mut typed = Vec::new();
    let mut idents = Vec::new();
    for arg in args {
        if let FnArg::Typed(pt) = arg {
            typed.push(quote! { #pt });
            if let Pat::Ident(pi) = &*pt.pat {
                let id = &pi.ident;
                idents.push(quote! { #id });
            } else {
                // Non-ident patterns are not supported; surface a clear error
                // at the call site by emitting a compile_error token.
                idents.push(
                    syn::Error::new_spanned(
                        &pt.pat,
                        "stage: actor method/fn parameters must be simple identifiers",
                    )
                    .to_compile_error(),
                );
            }
        }
    }
    (typed, idents)
}

/// Get the trailing identifier of a simple path type (e.g. `Counter`).
fn type_ident(ty: &Type) -> Option<Ident> {
    if let Type::Path(tp) = ty {
        return tp.path.segments.last().map(|s| s.ident.clone());
    }
    None
}

/// Extract `Actor` from a `ActorContext<'_, Actor>` type.
fn extract_actor_ty(ty: &Type) -> Option<Type> {
    if let Type::Path(tp) = ty {
        let seg = tp.path.segments.last()?;
        if seg.ident == "ActorContext" {
            if let PathArguments::AngleBracketed(ab) = &seg.arguments {
                for arg in &ab.args {
                    if let GenericArgument::Type(t) = arg {
                        return Some(t.clone());
                    }
                }
            }
        }
    }
    None
}

/// Rewrites an actor method body:
///
/// * `self.method(args)` where `method` is another async actor method becomes
///   `<SelfTy>::__stage_method_method(__ctx(), args)` — a direct inline call
///   that continues executing inside the *same* continuation (no new message,
///   same actor token). When the inline call suspends, the whole continuation
///   suspends and is resumed with the actor pointer re-published.
/// * Every other `self` (field access, sync helper calls) becomes the context
///   handle, which derefs to the actor.
struct RewriteBody<'a> {
    ctx: Ident,
    self_ty: Type,
    actor_methods: &'a HashSet<String>,
}

impl VisitMut for RewriteBody<'_> {
    fn visit_expr_mut(&mut self, e: &mut Expr) {
        match e {
            // `self.method(args)` for an async actor method -> inline call.
            Expr::MethodCall(mc)
                if is_self_expr(&mc.receiver)
                    && self.actor_methods.contains(&mc.method.to_string()) =>
            {
                let lowered = format_ident!("__stage_method_{}", mc.method);
                let self_ty = self.self_ty.clone();
                // Visit the args first so nested `self` uses are rewritten too.
                let mut args = mc.args.clone();
                for arg in args.iter_mut() {
                    self.visit_expr_mut(arg);
                }
                *e = parse_quote! {
                    <#self_ty>::#lowered(::stage::__private::__ctx(), #args)
                };
            }
            // Bare `self` -> context handle.
            Expr::Path(ep) if ep.qself.is_none() && ep.path.is_ident("self") => {
                ep.path = syn::Path::from(self.ctx.clone());
            }
            _ => syn::visit_mut::visit_expr_mut(self, e),
        }
    }
}

/// Is this expression exactly `self`?
fn is_self_expr(e: &Expr) -> bool {
    matches!(e, Expr::Path(p) if p.qself.is_none() && p.path.is_ident("self"))
}
