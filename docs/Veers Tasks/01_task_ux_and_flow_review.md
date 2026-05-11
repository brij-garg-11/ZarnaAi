# Task for Veer — UX, Flow & First-Impressions Review

_Assigned by: Brij_  
_Type: Observational research — no code changes, no testing_  
_Output: Create `02_veer_ux_review.md` in `docs/Veers Thoughts/` and send to Brij_

---

## What this task is

Zarna AI is a product that creators and small business owners use to run an AI-powered SMS fan engagement platform. We want a **fresh set of eyes** — not someone who built it — to walk through the product, do independent research, and tell us:

1. Does the flow make sense? Can you figure out what to do without anyone explaining it?
2. Does it look and feel like a real, premium product — or does it scream "built with AI tools"?
3. What are the best comparable products doing that we aren't?

This is pure observation and research. You are not QA testing. You are not filing bug reports. You are playing the role of a potential customer who is smart but skeptical, and then a design critic who has done homework.

---

## Before you start — context to absorb (30 min)

Read these in order. Don't skip.

1. **`docs/reviews/full-system-review.md`** — §1 (Website, UI/UX) only. Skim the rest for context. This gives you the technical lay of the land but deliberately leaves out opinions on design or UX feel.
2. **`docs/operations/veer_qa_test_plan.md`** — §1 and §2 only. Understand what the product is and who it's for. Ignore the test steps.
3. Visit these URLs and spend 5 minutes on each to understand the audience:
   - `https://zarna.ai` — the marketing site (not logged in)
   - `https://zarna.ai/performers` — the performer-facing page
   - `https://zarna.ai/business` — the business-facing page
   - `https://zarna.ai/how-it-works`
   - `https://zarna.ai/pricing`

**Two audiences we serve:**
- **Performers / Comedians / Creators** — people like Zarna Garg (comedian with 4,500 SMS subscribers). They want their fans to feel like they're texting the real her.
- **Small businesses** — places like a comedy club that want to engage their walk-in audience over SMS.

---

## Part 1 — Walk the flow as a first-time visitor (1-2 hours)

Do this without logging in. You are a creator who just heard about Zarna AI at a conference.

Go to `https://zarna.ai`. Start on the homepage. Navigate naturally — click what catches your eye, read what's there, go where the CTAs take you. Then come back and go through pages you missed.

As you do this, keep a running stream of consciousness. Write down every thought in real time, even if it sounds harsh. The goal is to capture reactions before familiarity dilutes them.

**Questions to hold in your head as you browse:**

- What does this product actually do? How many seconds does it take you to understand it from the homepage?
- Who is it for? Is that obvious or do you have to dig?
- What would make you trust this with your fans? What makes you doubt it?
- What's the single most compelling thing on the page? What's the weakest?
- If you were a comedian with 10,000 Twitter followers, would you sign up? What would stop you?
- Does anything feel confusing, out of order, or like it assumes knowledge you don't have?
- Where does the page lose your attention?
- What's missing that you expected to see?

Then walk through the signup flow (`/signup` → `/onboarding` → `/plans`) and note:

- Does each step make sense given the step before it?
- Are labels, inputs, and instructions written in plain English or in "startup jargon"?
- Is there a moment where you're lost or don't know what to do next?
- Does the pricing page give you enough information to decide?

---

## Part 2 — Research (2-3 hours)

Before forming opinions about what to fix, look at what the best products in adjacent spaces are doing. The goal is to build a visual and conceptual vocabulary so your feedback is grounded in real examples, not just personal taste.

### 2a — Direct or near-direct competitors to look at

Spend 10-15 min on each. Look at homepage, pricing page, and as far into the product as you can get without paying.

| Product | URL | What they do |
|---|---|---|
| Community | `https://www.community.com` | Creator-to-fan text messaging platform |
| Subtext | `https://subtext.com` | Influencer SMS subscription tool |
| SlickText | `https://www.slicktext.com` | SMS marketing for small businesses |
| Attentive | `https://www.attentive.com` | Enterprise SMS marketing |
| Postscript | `https://postscript.io` | SMS for e-commerce |
| Fan Reach | search "FanReach SMS" | Sports/entertainment fan SMS |

For each, write 3-4 sentences: What do they do well? What do they do poorly? What's one specific design decision you'd borrow?

### 2b — Products known for great dashboard UX (not SMS-specific, just great dashboards)

| Product | URL | Why relevant |
|---|---|---|
| Linear | `https://linear.app` | Clean, fast, opinionated SaaS UI — widely praised |
| Lemon Squeezy | `https://www.lemonsqueezy.com` | Creator economy payments — beautiful marketing + dashboard |
| Beehiiv | `https://www.beehiiv.com` | Newsletter platform for creators — excellent onboarding |
| Loops | `https://loops.so` | Email for SaaS startups — great UI, creator-friendly |
| Raycast | `https://www.raycast.com` | Software product, but legendary landing page |

For each: what specifically do they do visually or in terms of flow that you'd want to steal?

### 2c — "AI-looking" vs "real product" — research exercise

This is the most important part. "AI-built" products have a recognizable visual signature that makes them feel cheap:

- Same 3 fonts used everywhere
- Over-reliance on purple/indigo gradients
- Glassmorphism cards that look like a UI kit template
- Generic hero copy ("AI-powered platform for your business")
- Robot icons and circuit-board imagery
- Blob shapes and floating orbs as decoration
- Testimonials that look like they were written by GPT
- Pricing tables that all look identical
- Animated number counters on the homepage

Go to `https://zarna.ai` and answer honestly: which of these patterns does the site have? Which don't fit the brand (a comedian who connects with fans)?

Then look at these products known for NOT looking AI-built and describe what they do differently:

- `https://www.patreon.com`
- `https://www.superhi.com`
- `https://gumroad.com`
- `https://www.pika.art`
- `https://liveblocks.io`

What do human-feeling design choices look like? (Hint: often it's irregular typography, photography of real people, editorial voice, bold color that's unconventional, imperfection as a design choice.)

### 2d — What Zarna Garg's brand actually looks and feels like

Do 20 minutes of research on the actual creator:

- Google "Zarna Garg comedian"
- Watch one or two of her Instagram Reels or YouTube clips
- Look at her website if she has one
- Read a few fan comments

Answer: what visual and tonal energy does she project? Does the Zarna AI product feel like it belongs in her world? Or does it feel like a generic SaaS product that has her name slapped on it?

---

## Part 3 — Logged-in product flow (30-45 min, if Brij gives you a test login)

If Brij sets you up with a test account, log in and walk the main dashboard areas as a **new user on day 1** who just signed up:

- What's the first thing that draws your eye?
- Is it obvious what the product is doing for you?
- Does the navigation make sense? Are labels intuitive?
- Is there anything that feels like it belongs to a different product (inconsistent visual style)?
- Does anything feel like an afterthought or was clearly added later?
- What would make you feel confident the bot is working vs uncertain?

You don't need to click every button or test for errors. Just walk it like a new customer would.

---

## Part 4 — Write your output

Create a new file: **`docs/Veers Thoughts/02_veer_ux_review.md`** — this is your submission folder, not the tasks folder.

Structure it however makes sense for your thoughts, but include at minimum these sections:

### A. First impressions (homepage + marketing site)
- The moment-by-moment stream of consciousness from Part 1
- What landed, what didn't
- What you'd cut, move, or rewrite

### B. The flow
- Does the overall journey (land → understand → sign up → onboard → use) make sense?
- Where does it break down or lose momentum?
- What's the biggest friction point a first-time creator would hit?

### C. Competitor observations
- One paragraph per competitor you looked at (the short list from 2a)
- 3-5 specific things you'd borrow (with links to screenshots or examples if you can find them)

### D. "AI-looking" audit
- Which specific UI patterns on `zarna.ai` feel generic/AI-built?
- Screenshots or specific page sections where it's most obvious
- Reference examples from the non-AI-looking products you researched

### E. Brand alignment
- Does the product feel like Zarna Garg? Why or why not?
- What would it look like if the product had her energy?

### F. Proposed changes (your actual recommendations)
Be specific. Not "make it more human" — more like:

> "The hero headline 'AI-powered fan engagement platform' could be rewritten in Zarna's actual voice. Something like: 'Your fans are texting. What if you could actually reply to all of them?' This is more conversational and matches how she talks."

Or:

> "The pricing page uses purple gradient cards that look identical to 10 other AI products I looked at. Lemon Squeezy uses soft warm neutrals with bold type and no gradients — it feels more like a business tool made by people who care. We should consider this direction."

At least 5 specific, actionable recommendations.

### G. Questions for Brij
Anything you're unsure about, need context on, or want to pressure-test with him before finalizing your recommendations.

---

## Deliverable

A single MD file: `docs/Veers Thoughts/02_veer_ux_review.md`

No length minimum or maximum — as long as it takes to say what you actually think. Bullet points are fine. Screenshots can be linked or described. Harsh opinions are welcome; this is more useful than polite opinions.

Send to Brij when done.
