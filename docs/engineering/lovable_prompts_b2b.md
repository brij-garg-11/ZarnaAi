# Lovable prompts — B2B "Apply for access" frontend conversion

These prompts convert the public funnel from self-serve signup → onboarding
wizard into a **B2B apply-for-access** flow. The backend is already built and
tested (see `docs/plans-to-complete/10_b2b_request_access_conversion.md`).

Submit them to Lovable **one at a time, in order**, and verify after each.

Backend API contract you can rely on:

- `POST {API_BASE}/api/access-request` (public, no auth)
  body: `{ name, email, account_type: "performer" | "business", link, goal, phone, source }`
  → `200 { success: true }` or `{ success: false, error }`
- `POST {API_BASE}/api/auth/signup` now returns **403** `{ apply_required: true }` (closed).
- `POST {API_BASE}/api/onboarding/submit` now returns **403** for non-admins.
- Approved clients receive an email link to `/reset-password?token=...` to set their password.

---

## Prompt 1 — Build the public "Apply for access" page

> Add a new page at route `/apply` called "Apply for access".
>
> It's a single, centered form on the marketing theme with this heading:
> "Apply for access" and subtext "Tell us about you and we'll build your AI
> texting bot. We'll email you when it's ready."
>
> Fields:
> - Full name (required)
> - Email (required, email validation)
> - "I am a…" select with two options: "Creator / Performer" (value
>   `performer`) and "Business" (value `business`). Default `performer`.
> - "Website or main social link" (text, optional)
> - "What are you looking to do?" (multiline textarea, optional)
> - Phone (optional)
>
> On submit, POST JSON to `${API_BASE}/api/access-request` with
> `{ name, email, account_type, link (the website/social field), goal (the
> textarea), phone, source: "website" }`, `credentials: "include"`.
> On success show a confirmation state: "You're on the list — we'll be in
> touch by email." Hide the form after success. On error show the returned
> `error` message. Import `API_BASE` from `@/components/dashboard/types`.
>
> Add an invisible honeypot field named `company_website` that, if filled,
> silently no-ops the submit (basic bot protection).

## Prompt 2 — Re-point every "Create a bot" CTA to /apply

> Replace the `useCreateBot` hook in `src/lib/create-bot.ts` so that, for a
> visitor who is NOT already a logged-in customer with a bot, it navigates to
> `/apply` instead of `/signup` or `/onboarding`.
>
> New behavior: call `GET ${API_BASE}/api/onboarding/status` with credentials.
> - If `200` and `completed === true` → `navigate("/dashboard")`.
> - In every other case (401 unauthenticated, or completed === false) →
>   `navigate("/apply")`.
>
> Keep the hook's name and signature identical so all existing CTA buttons keep
> working. Do not change anything else.

## Prompt 3 — Remove the public signup + onboarding wizard from the funnel

> The product is now invite-only / apply-for-access. Make these changes:
> - Remove the public "Sign up" / "Create account" call-to-action buttons and
>   any links pointing to `/signup` and `/onboarding` from the marketing site
>   (navbar, landing hero, footer). Replace them with a single "Apply for
>   access" button linking to `/apply`.
> - Keep the "Log in" button and the `/login` page exactly as they are.
> - If routes `/signup` and `/onboarding` still exist, make them redirect to
>   `/apply` (don't delete the dashboard or login).
> - The signup API now returns 403; remove the email/password signup form UI.

## Prompt 4 — Remove all pricing & billing from the public site

> Remove every public-facing pricing, plans, and billing element:
> - Delete the pricing page/section and any "Pricing" nav links.
> - Remove plan tiers, price tables, "Start free trial", and credit/usage
>   displays from the marketing pages.
> - Inside the logged-in dashboard, hide any billing/credits/upgrade UI.
> Keep the login and dashboard otherwise intact.

## Prompt 5 — Set-password (invite) page copy

> The password-reset page at `/reset-password` is reused as the "set your
> password" page for newly approved clients (they arrive via an emailed link
> `?token=...`). When there is a token in the URL, show heading "Set your
> password" and button "Set password & continue" (instead of "Reset
> password"). On success, redirect to `/dashboard`. Keep the existing token
> POST to `${API_BASE}/api/auth/reset-password` unchanged.

## Prompt 6 — Rebrand the whole site to "twowaybot"

> Rebrand the entire site from "Zar" / "Zarna" to **twowaybot**:
> - Replace the logo/wordmark and all visible occurrences of the old brand
>   name in the navbar, hero, footer, page titles, and meta tags with
>   "twowaybot".
> - Update the browser tab title and meta description.
> - Centralize the brand name in one constant (e.g. `src/lib/brand.ts`
>   exporting `BRAND = "twowaybot"`) and reference it everywhere so future
>   renames are one line.
> - Do not change API URLs or the `API_BASE` value.

## Prompt 7 — Fix the leftover "ZarBot" logo on auth + dashboard screens

> The marketing header/footer now say "twowaybot", but there's still a
> hardcoded old wordmark elsewhere: the text **"Zar"** immediately followed by
> **"Bot"** rendered in the accent color (`<span>Zar</span><span
> class="text-accent">Bot</span>`). It appears on:
> - the Apply page card (`/apply`)
> - the Login page
> - the Reset Password and Forgot Password pages
> - the dashboard sidebar/header (the `DashboardShell`)
>
> Replace **every** instance of this "Zar"+"Bot" wordmark with the same
> "twowaybot" wordmark used in the marketing site header. Centralize it into a
> single shared `<Logo />` component (reading `BRAND` from `src/lib/brand.ts`)
> and use that everywhere so the brand only lives in one place.
>
> Do NOT change API URLs, the `API_BASE` / `https://api.zar.bot` value, or any
> localStorage keys (e.g. `zar.viewingAs`, `zar.previewAccountType`).

---

## Verify after submitting

1. Landing page primary CTA → `/apply` (not signup/onboarding).
2. `/apply` form submits and shows the success state.
3. A new lead appears in Operator HQ → **Leads** tab.
4. `/login` still works and lands existing customers on `/dashboard`.
5. No pricing/billing anywhere public.
6. Brand reads "twowaybot" throughout — including the Apply card, Login,
   Reset/Forgot Password, and the dashboard sidebar/header (no "ZarBot").
