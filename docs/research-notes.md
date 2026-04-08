# Research notes

Greyline exists because real-world structured data is often only structured in the most technical sense of the word. The files open. The rows parse. The columns have names. None of that means the semantics are stable, obvious, or safe to trust.

This tool is built around a simple doctrine:

- the model proposes
- deterministic code validates
- humans approve consequential meaning

## Why the model is bounded

The model is useful for one narrow job: looking at compact field evidence and suggesting what a record probably represents. That is very different from letting a model interpret evidence at scale.

Greyline uses the model once, on a bounded sample, to answer questions like:

- what record family does this look like
- which fields probably correspond to the ontology
- where is the ambiguity
- which mappings look weak enough to hold back

That is a sensible use of a small local model. It is not a sensible use of a model to stream through a full multi-gigabyte export and silently invent meaning row by row.

## Why validation overrides the model

A plausible answer is not the same thing as a trustworthy answer.

Field names lie. Exports drift. Providers change conventions. Time fields are often ambiguous. One system's "owner" is another system's subscriber, handset user, or account holder.

Greyline therefore treats model output as a candidate mapping proposal, not as truth. Deterministic checks exist to stop the most obvious forms of semantic nonsense from being promoted.

Examples:

- a field mapped to `*.msisdn` should look like a phone number
- a field mapped to a timestamp should parse as one
- latitude and longitude should be numeric and in range
- duplicate target mappings should be treated with suspicion

The point is not perfection. The point is to avoid confident rubbish.

## Why correspondence is richer than equality

Greyline uses relations such as `exact`, `close`, `broad`, `narrow`, and `unknown` because real mappings are often not clean one-to-one equals signs.

A source field may be:

- very likely the exact target field
- close, but with provider-specific interpretation
- broader than the target concept
- narrower than the target concept
- too ambiguous to claim either way

That distinction matters. Pretending every decent-looking match is exact is how you poison downstream analysis while still feeling clever.

## Why provenance is first-class

A canonical field without lineage is just a confident assertion.

Greyline keeps provenance because the useful question is not only "what value is in this field", but also:

- which source field produced it
- what raw value was seen
- what transform was applied
- which mapping version was used
- which validator logic touched it

That is basic engineering hygiene for any environment where repeatability, audit, or evidential defensibility matters.

## Why quarantine exists

Bad rows should not force a whole ingest run to fail, and they should not be silently shoved through either.

Greyline splits accepted output from quarantined output so that:

- good rows keep moving
- bad or ambiguous rows are preserved
- failure reasons are visible
- drift becomes measurable instead of anecdotal

A mature pipeline does not just say "it worked" or "it failed". It should tell you how much of the run was clean, how much was coerced, and how much was held back because trust was not good enough.

## Why streaming matters

For schema discovery, there is no reason to drag a 5 GB export wholesale into Python memory just to decide what four columns probably mean.

Greyline separates discovery from ingest:

- discovery works on bounded samples
- ingest applies approved mappings deterministically at scale

That keeps the expensive semantic work small and allows the heavy lifting to remain boring, which is exactly what you want in production.

## Why family-aware ingest exists

Large ugly exports are often not one clean repeated schema. They may contain:

- more than one record family
- partial sections
- footer junk
- source-specific weirdness
- inconsistent population of fields across rows

Greyline therefore supports family-aware ingest so one file does not have to pretend to be one thing. Rows that fit no known family strongly enough are quarantined rather than being rammed into the least bad option.

## The failure mode to avoid

The dangerous failure mode here is not usually a crash.

It is plausible but wrong data.

That is the whole reason Greyline is opinionated about validation, provenance, quarantine, and governance. A pipeline that fails loudly is annoying. A pipeline that quietly emits polished nonsense is much worse.

## Practical stance

Greyline is not a casework platform, not an investigative reasoning engine, and not an excuse to sprinkle AI over poor data governance.

It is a narrow tool for one awkward problem:

- recognise likely schema meaning
- preserve uncertainty
- validate aggressively
- promote approved mappings
- process large files without melting the machine
- keep the chain from source to canonical output visible

That is enough.
