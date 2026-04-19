# Lovable Frontend Wiring Reference

Everything the Lovable (React) app needs to wire up correctly to the Flask backend.

---

## Railway environment variables (set on the operator web service)

| Variable | Value | Why |
|---|---|---|
| `FRONTEND_URL` | `https://zar-fan-connect.lovable.app` (or your current Lovable URL) | Controls where Google OAuth redirects back to and where `/onboarding` redirects land after signup |
| `GOOGLE_REDIRECT_URI` | `https://zarnaai-production.up.railway.app/api/auth/google/callback` | Must match what is registered in Google Cloud Console |
| `CORS_ALLOWED_ORIGINS` | Include your Lovable preview URL if it changes | All `*.lovable.app` subdomains are already allowed via regex — only needed if you add a custom domain |

---

## Every fetch call must include `credentials: "include"`

The backend uses a session cookie, not a bearer token. Without `credentials: "include"` the browser never sends the cookie on cross-origin requests and every protected route returns 401.

```js
// Required on every API call
fetch("https://zarnaai-production.up.railway.app/api/...", {
  method: "GET",            // or POST, etc.
  credentials: "include",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(payload),  // for POST/PUT
})
```

---

## Session check on app load

```
GET /api/auth/me
```

Response when logged in:
```json
{
  "authenticated": true,
  "user": {
    "email": "user@example.com",
    "name": "Jane",
    "is_owner": false,
    "account_type": "performer",
    "creator_slug": "jane",
    "is_super_admin": false
  }
}
```

Response when not logged in: `401 { "authenticated": false }`

**Logic:**
- 401 → redirect to `/login`
- `creator_slug` is null or empty → redirect to `/onboarding`
- Otherwise → show dashboard

---

## Auth endpoints

### Email/password signup
```
POST /api/auth/signup
Body: { "email": "...", "password": "...", "name": "..." }
```
Success: `{ "success": true, "onboarding_required": true, "user": {...} }` → redirect to `/onboarding`

### Email/password login
```
POST /api/auth/login
Body: { "email": "...", "password": "..." }
```
Success: `{ "success": true, "redirect_to": "/operator/dashboard", "user": {...} }`

### Google OAuth
```
GET /api/auth/google           → redirects to Google
GET /api/auth/google/callback  → handled by backend; redirects to FRONTEND_URL/dashboard or FRONTEND_URL/onboarding
```
To initiate signup via Google: `GET /api/auth/google?signup=true`

### Logout
```
POST /api/auth/logout
```
Success: `{ "success": true }` → redirect to `/login`

---

## Onboarding wizard call sequence

### Step 0 — gate check
```
GET /api/onboarding/status
```
Response: `{ "completed": false, "account_type": null, "creator_slug": null }`
- `completed: false` → show wizard
- `completed: true` → redirect to `/dashboard`

### Step 4 (final wizard step) — submit
```
POST /api/onboarding/submit
Body:
{
  "account_type":  "performer" | "business",
  "display_name":  "Jane Smith",
  "slug":          "jane",           // auto-suggested from name; user can edit
  "bio":           "...",
  "tone":          "casual" | "professional" | "hype" | "warm",
  "website_url":   "https://...",
  "podcast_url":   "https://...",
  "media_urls":    ["https://...", ...],
  "extra_context": "Anything else the AI should know..."
}
```
Success: `{ "success": true, "creator_slug": "jane", "account_type": "performer" }` → redirect to `/dashboard`

Conflict (slug taken): `409 { "success": false, "error": "The name 'jane' is already taken." }`

---

## Bot settings page

### Load current config
```
GET /api/bot-data
```

Performer response shape:
```json
{
  "name": "Jane Smith",
  "bio": "...",
  "tone": "casual",
  "voice_style": "...",
  "website_url": "...",
  "podcast_url": "...",
  "media_urls": [],
  "links": { "tickets": "", "merch": "", "book": "", "youtube": "" },
  "banned_words": [],
  "name_variants": [],
  "edits_used": 0,
  "edits_limit": 20
}
```

Business response shape:
```json
{
  "display_name": "West Side Comedy Club",
  "business_type": "comedy club",
  "tone": "casual",
  "welcome_message": "...",
  "signup_question": "...",
  "website": "...",
  "address": "...",
  "hours": "...",
  "tracked_links": {},
  "edits_used": 0,
  "edits_limit": 20
}
```

### Save changes
```
POST /api/bot-data
Body: { /* only the fields you want to change */ }
```

Performer allowed fields: `name`, `bio`, `description`, `tone`, `voice_style`, `website_url`, `podcast_url`, `media_urls`, `banned_words`, `links`

Business allowed fields: `tone`, `welcome_message`, `signup_question`, `outreach_invite_message`, `address`, `hours`, `website`, `tracked_links`, `display_name`

Success: `{ "success": true }`

---

*Last updated: April 2026*
