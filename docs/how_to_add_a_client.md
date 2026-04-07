# How to Add a New Client

**Who this is for:** Anyone on the team helping bring on a new creator client.  
**What this covers:** Everything from first conversation to the day they go live — what you need from them, what we do internally, what they get, and what they're responsible for.

---

## The Big Picture

We are a **fully managed AI texting service** for comedians and creators. When we take on a client:

- Their fans text a phone number and get back replies that sound exactly like them
- They get a private dashboard to see their audience, run live show signups, and send mass texts
- We own and run everything behind the scenes — hosting, AI, phone infrastructure, fan database
- They never touch a server, never manage software, never think about any of it

**Our job is to make it look effortless on their end.**

---

## Phase 1 — Before You Sign Them

### What to confirm before committing

Make sure you can answer yes to all of these before moving forward:

- [ ] Do they have an active audience? (Touring comics, podcasters, YouTubers with real fan engagement — not someone just starting out)
- [ ] Do they have existing content we can train the AI on? (At least one of: a YouTube channel, a podcast, a special, a book)
- [ ] Are they willing to publish an SMS privacy policy and terms on their website? (This is legally required for SMS marketing — they must do this)
- [ ] Are they willing to be the one who approves every mass text before it goes out? (We send it, but they sign off on the content every time)
- [ ] Have they confirmed their existing subscriber list (if any) has valid opt-in consent? (If they're importing old contacts, those fans must have agreed to receive texts)

---

## Phase 2 — What to Collect From the Client

This is your intake. You need all of this before we can start building anything. Send them a simple request list.

### Their Content (for training the AI)

The AI learns from their real material. The more we have, the better it sounds like them.

| Content Type | What to Ask For | Priority |
|---|---|---|
| YouTube channel | Link to their YouTube page, or specific video links | High |
| Podcast | Name of the podcast or RSS feed link | High |
| Stand-up specials | Any video links or transcript files | High |
| Book | PDF of the book, or a link | Medium |
| Written work | Newsletter, Substack, articles, blog posts | Medium |
| Instagram / TikTok | Links to their profiles | Low |

> **Minimum to launch:** They need at least one high-priority source above. Ideally two or more. The AI will sound thin with very little material.

### Their Voice (for writing their AI personality)

We write the AI personality ourselves after studying their material — but it helps to hear it from them directly too. Ask them:

1. **How would you describe your comedy style in 2-3 sentences?** (Example: "I'm the sharp, Indian-immigrant mom who tells the truth your family won't.")
2. **What topics are completely off-limits?** (Example: a past relationship, a pending legal matter, a family member who's passed away)
3. **What are 5-10 phrases you actually use with fans?** (DMs, comments, replies — real stuff, not what they think they'd say)
4. **Who is the AI talking to?** (Who is a typical fan — age, vibe, why they follow this creator)

### Their SMS Setup

| Item | What to Ask | Notes |
|---|---|---|
| Do they have a SlickText account? | Yes / No | If yes, we need the API key. If no, we create one. |
| What keyword do they want? | A word fans text to subscribe | Should match their brand. Example: ZARNA, SARAH, FUNNY |
| Do they have an existing subscriber list? | Yes / No | If yes, we need the file AND their confirmation that consent is valid |
| What phone number do they want to text from? | Their preference | We handle getting it — just need to know if they have a preference |

### Their Legal Items (they must do this themselves)

We cannot launch without these in place. These are their responsibility, not ours — but we need to confirm they exist before going live.

- [ ] **Privacy Policy** — published on their website, covers SMS data collection
- [ ] **SMS Terms of Service** — explains message frequency, how to opt out (STOP), that messages are automated
- [ ] **Confirmation that existing subscriber consent is valid** — if they have an existing list

> **Who writes these?** They do, with their own legal counsel. We can point them to the compliance overview in `docs/sms_compliance_sections.md` as a reference. We do not write their legal documents.

---

## Phase 3 — What We Do Internally

This is all Brij / the technical team. The rest of the team doesn't need to do any of this — just know what's happening and roughly how long it takes.

### 3A. Set Up Their Client Folder (~15 minutes)

We create a client record in our system:
- Copy the client template in `clients/template/` → `clients/[their name]/`
- Fill out their info: who they are, what content we have, deployment notes
- Copy the voice config template → `creator_config/[their name].json`

### 3B. Ingest Their Content (~30 minutes, mostly automated)

We run scripts that pull in all their content — YouTube videos, podcast episodes, specials, book. The scripts do the heavy lifting. At the end we have a knowledge base file that contains everything the AI knows about them.

### 3C. Write Their Voice Config (~2 hours, this is the craft work)

This is the most important step. We watch/read their material and write the AI's personality guide — their energy, what angles they take, how they handle emotional fan messages, what they never say. This is what makes the AI sound like *them* instead of a generic bot. It takes real care to get right.

### 3D. Deploy Their System (~30 minutes)

We spin up a fresh copy of our platform on Railway (our hosting provider) pointed at their configuration. They get:
- Their own AI texting system connected to their phone number
- Their own private dashboard at a URL we give them
- Their own isolated database — their fan data never touches another client's

### 3E. Test (~1 hour)

We text their number 20+ times pretending to be a fan. We test normal fan messages, edge cases, their specific topics, the keyword signup flow, the STOP/unsubscribe flow. When it passes our QA, we hand it to the creator.

### 3F. Creator Signs Off

The creator texts the number themselves. They decide if it sounds like them. We do not go live until they say yes. **Get that approval in writing — a Slack message or email is fine.**

---

## Phase 4 — Going Live

### What "going live" means

- The keyword is announced publicly (at a show, on Instagram, in a newsletter, wherever)
- Fans start texting
- The AI starts responding
- The dashboard is active

### Launch options

| Launch Method | Best For |
|---|---|
| Announce at a live show | Immediate surge — 200-500 new subscribers in one night |
| Post keyword on Instagram / TikTok | Gradual rollout over days |
| Email newsletter | Existing email list converting to SMS |
| All at once | Use all three together for maximum impact |

### What we monitor at launch (first 48 hours)

- Are texts coming in and getting replied to?
- Are there any AI errors showing up in logs?
- Are the replies sounding right?
- Is the keyword signup working?

---

## Phase 5 — Ongoing

### What the client does after launch

| Task | How Often | Where |
|---|---|---|
| Check their dashboard (subscriber count, engagement) | Whenever they want | Their dashboard URL |
| Create a new live show for an upcoming event | Before each show | Dashboard → Shows |
| Send a mass text to their audience | Whenever they want | Dashboard → Blast |
| Download their subscriber list | As needed | Dashboard → Audience |

### What we do after launch

| Task | How Often | Who |
|---|---|---|
| Monitor for AI errors or unusual behavior | Ongoing (automated) | Brij |
| Add new content when they release something new (new special, new podcast episodes) | When they release it | Brij |
| Update their voice config if they flag something is off | As needed | Brij |
| Renew/manage hosting and SMS accounts | Monthly | Brij |

### What the client should tell us about

- They released a new special, new episodes, a new book → we need to update the AI
- A fan sent something concerning (threat, mental health crisis) → we review conversation logs
- Something feels off about how the AI is responding → we fix the voice config
- They want to add a team member to the dashboard → we create a login for them

---

## Timeline Summary

From signed contract to live:

| Phase | Who Does It | Time Needed |
|---|---|---|
| Collect content + intake form | Client (with your help) | 1-3 days |
| Write voice config | Brij | 2 hours |
| Ingest content + build AI | Brij | 1-2 hours (mostly automated) |
| Deploy system | Brij | 30 minutes |
| Test + QA | Brij | 1 hour |
| Creator signs off | Creator | Same day, usually |
| **Total from intake to live** | | **3-5 business days** |

> The client's speed on providing their intake is usually the longest part of the timeline.

---

## What We Control vs. What They Control

### We own and run
- Everything technical (hosting, AI, database, SMS infrastructure)
- Their training data and knowledge base
- All security and credentials
- Uptime and reliability

### They control (through their dashboard)
- Creating live show events and choosing keywords
- Writing and sending mass texts (they write it, we send it — they must confirm before every send)
- Viewing their audience and conversation history
- Downloading their subscriber list

### They are solely responsible for
- Their privacy policy
- Their SMS terms of service
- The validity of their subscriber consent
- Any legal matters involving their content or fanbase

---

## If Something Goes Wrong

| Situation | What to Do |
|---|---|
| AI gives a weird or off-brand reply | Note the fan's number and approximate time, send to Brij to review |
| Mass text went to wrong audience | Contact Brij immediately — do not send anything else |
| Client can't log into dashboard | Send to Brij — he resets the login |
| Fan is texting something concerning (threats, crisis) | Flag to Brij — he can pull the conversation log |
| Client wants to pause the AI temporarily | Contact Brij — this is a settings change on our end |
| Fan complains they can't unsubscribe | Tell them to text STOP — the system handles it automatically |

---

## Quick Reference Card

**Before signing a client:**
> Confirm they have content, an audience, and are willing to handle their own legal compliance.

**What we need from them:**
> Content links + voice description + SMS keyword + privacy policy confirmation.

**What we do:**
> Ingest content → write their AI voice → deploy their system → test it → hand it to them.

**Timeline:**
> 3-5 business days from complete intake to live.

**After launch:**
> We monitor and maintain. They log in to their dashboard and run their audience.

---

*Last updated: April 2026. For questions about any step, contact Brij.*
