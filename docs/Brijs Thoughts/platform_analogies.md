# Platform Architecture — Analogies
_Plain-English explanations of how the bot creation pipeline works_
_Apr 21, 2026_

---

## The Platform is a Restaurant Franchise

Think of Zar as a franchise like Chipotle. Every location (every creator's bot) uses the **same kitchen equipment, same ordering system, same recipe process** — but each location serves completely different food made from completely different ingredients.

- The **kitchen equipment** = our Flask brain, Gemini API, intent classification, retrieval logic
- The **recipe card** = `creator_config/<slug>.json` — tells the kitchen how *this* location's food should taste
- The **ingredients** = the embeddings in `creator_embeddings` — the actual content the bot draws from
- The **dining room** = the fan's text conversation — they never see the kitchen

A Chipotle in Austin and a Chipotle in New York use the same grill. The grill doesn't care. What comes out is completely different because the recipe card and the ingredients are different.

---

## Voice Separation is Like an Actor Reading a Script

The LLM (Gemini/GPT) is an actor. It can play anyone. But it needs a **script and character brief** at the top of every scene.

Right now, most scenes start with: *"You are playing Zarna Garg."* Even if the rest of the script has Haley's lines in it.

The fix: every scene starts with *"You are playing [whoever's config is loaded]."* The actor doesn't care — they just need the right character brief. The problem isn't the actor, it's that some of the scene headers still say the wrong name.

---

## RAG is Like a Personal Assistant With a Filing Cabinet

When a fan asks HaleyBot "when's your next show?" — the bot doesn't just guess. It sends an assistant to a **filing cabinet** first.

- `EmbeddingRetriever` (Zarna's current system) = one big shared filing cabinet where everything is Zarna's files. If Haley's assistant goes in there, they'd come back with Zarna's tour dates.
- `PgRetriever` (what we're building) = every creator has their own **locked drawer** inside the cabinet, labeled with their name. Haley's assistant can only open Haley's drawer. Physically impossible to pull Zarna's files.

The query embedding (converting "when's your next show?" into a vector) is like the assistant knowing **what kind of document to look for**. The `WHERE creator_slug = 'haley'` is the **lock on the drawer**.

---

## The Config File is Like a Character Bible

Every TV show has a character bible — a document that says: here's who this character is, how they speak, what they'd never say, what their relationships are, sample dialogue.

`creator_config/zarna.json` is Zarna's character bible. `creator_config/haley.json` will be Haley's.

The LLM reads the character bible before every single reply. It doesn't remember anything between conversations — it's like an actor who starts fresh every scene. So the bible has to be thorough, because that's the *only* thing it has to go on.

The **voice leakage risk** is: if Haley's character bible is incomplete or missing sections, the system falls back to Zarna's bible. So Haley's bot would start referencing Zarna's husband, Zarna's mother-in-law, and Zarna's opinions. It's like an actor getting halfway through Haley's character bible and then accidentally picking up Zarna's.

---

## Provisioning is Like Opening a Franchise Location

When a new creator signs up, the platform does the equivalent of:

1. **Lease the space** — buy them a Twilio phone number, wire it to their bot
2. **Write their recipe card** — LLM reads their bio and generates their `creator_config` personality file
3. **Stock the kitchen** — scrape their website, chunk it, embed it, load it into their drawer in the filing cabinet
4. **Hang the sign** — send them the "you're live" email with their number

All four happen automatically in the background in ~70 seconds. The creator just filled out a form. They had no idea any of this was happening.

---

## The Slug is Like a Social Security Number

Every creator gets a `slug` — a short unique identifier like `zarna`, `haley`, `wscc`. It travels with every single piece of data:

- `bot_configs WHERE creator_slug = 'haley'` — settings
- `contacts WHERE creator_slug = 'haley'` — fans
- `creator_embeddings WHERE creator_slug = 'haley'` — knowledge
- `creator_config/haley.json` — personality
- Twilio number wired to `/smb/inbound?tenant=haley` — incoming texts

The slug is the thread that ties all of it together. Lose the slug or mix it up, and you're handing Haley's fan data to the wrong creator. Get it right, and it's impossible for any piece of one creator's system to accidentally touch another's.

---

## The Current State (Plain)

Right now it's like a franchise where the recipe card system is built, the filing cabinet drawers are built, the kitchen works perfectly — but whoever typed the scene headers for 7 out of 9 scenes forgot to use the variable and just wrote "Zarna Garg" directly. It's a find-and-replace fix, not a redesign. Everything else is already wired correctly.
