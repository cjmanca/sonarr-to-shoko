# Sonarr-to-Shoko — User Experience Redesign

Repository: https://github.com/cjmanca/sonarr-to-shoko

## Product direction

The application's current user experience is unacceptable for its intended audience.

It exposes approximately 27 CLI commands, complex YAML configuration, manual safety attestations, campaign preparation, approval digests, canary operations, and recovery procedures.

This is an engineering/operator interface, not a consumer-facing application.

**The new primary goal is a set-and-forget background service.**

A typical user should be able to install the Docker container, provide Sonarr and Shoko connection details/API keys, and have the application automatically link eligible anime episodes without further routine interaction.

The existing safety and recovery systems should become internal implementation details.

This is an intentional change to the product requirements. Update `PROJECT.md` to reflect the new user experience. Create a separate implementation plan for this redesign rather than modifying the previously frozen `PLAN.md`.

Follow the existing `AGENTS.md` and `ORCHESTRATION.md` conventions.

## 1. Introduce a simplified default operating mode

The normal installation should require only:

* Sonarr URL and API key.
* Shoko URL and API key.

Support configuration through Docker environment variables and provide an Unraid-friendly Docker template.

Avoid requiring users to create or edit a large YAML configuration file.

Advanced configuration may remain available but must be optional.

The application should start its continuous background service automatically when the container starts.

The normal mode should handle both the existing unrecognized library and newly arriving episodes.

Do not expose campaigns, attestations, approval digests, or reconciliation terminology to normal users.

## 2. Automatically discover the environment

On startup, use the Sonarr and Shoko APIs to discover:

* Sonarr anime series and their root folders.
* Shoko import folders.
* Candidate files present in both systems.
* Differences between Sonarr and Shoko container paths.
* Compatible API versions and available capabilities.

Automatically establish path mappings when sufficient evidence exists.

Use exact file paths, relative path structure, file sizes, and other available identity information to validate inferred mappings.

Never silently guess ambiguous path mappings.

Do not accidentally include live-action TV or Shoko's separately managed manual collection.

When automatic discovery cannot establish a safe scope, request one simple user decision.

For example:

"Which Sonarr folder contains your anime?"

Do not require users to understand Docker path translation unless automatic discovery genuinely fails.

## 3. Replace routine manual attestations with automatic safety checks

The current expiring safety-attestation workflow is incompatible with an unattended application.

Investigate which Shoko safety settings and capabilities can be reliably inspected through its supported API.

Automatically validate everything that can be verified.

This includes the settings governing file deletion, renaming, movement, and other potentially destructive linking side effects.

**Do not automatically change Shoko's configuration without user consent.**

The application must preserve these existing protections:

* Never overwrite existing Shoko episode associations.
* Never submit an ambiguous episode mapping.
* Never link an out-of-scope file.
* Never blindly repeat an uncertain POST.
* Never intentionally modify media files.
* Stop linking when Shoko's behavior cannot be considered safe.

For facts that cannot be established programmatically, provide a straightforward one-time safety confirmation during setup.

Do not require users to repeatedly execute CLI commands or recreate attestations every seven days.

Re-evaluate automatically observable safety conditions whenever appropriate, including after Shoko version changes.

A new user should not need to learn what an attestation is.

Importantly, do not bypass physical-media safety requirements by automatically generating fictitious operator attestations.

## 4. Make backlog processing automatic

The existing campaign system can remain internally, but the user should not have to operate it.

The application should automatically identify eligible unrecognized files and maintain a persistent work queue.

Reuse the existing:

* Frozen file identities.
* Immutable mapping targets.
* Durable SQLite submission records.
* Protected-link preflight.
* Queue and submission budgets.
* Asynchronous verification.
* Restart recovery.
* Duplicate-submission prevention.

Automatically process an initial small batch and verify the resulting Shoko associations before gradually increasing processing.

Only do this after the required environment safety checks have passed.

If physical-media safety cannot be established, do not automatically perform the initial write.

No manual campaign preparation, approval digest, canary verification, or drain authorization should be necessary in the normal verified-safe operating mode.

The user should not have to calculate campaign deadlines.

Normal background work should not expire simply because it takes longer than seven days.

Handle mapping updates and temporary API failures automatically.

Individual ambiguous or conflicting files should be skipped and reported without unnecessarily stopping other safely processable files.

## 5. Improve the default processing rate

The existing default rolling budget of 200 submissions per week is far too conservative for a typical backlog of several thousand episodes.

Review the actual rate-limiting requirements.

Use conservative but practical defaults that consider:

* Shoko's processing queue.
* Outstanding asynchronous jobs.
* Verification results.
* API failures and throttling.
* Detected anomalies.

Start cautiously and increase throughput only when Shoko is responding normally.

Slow down or stop automatically when the system encounters problems.

Do not remove the durable submission limits or uncertain-send protections.

Users should not need to configure rate limits to obtain reasonable backlog processing.

## 6. Create a minimal user-facing interface

Provide a simple way to inspect operation without learning CLI commands.

A lightweight local web dashboard is preferable for ordinary Docker users.

It should show:

* Connection status.
* Number of successfully linked files.
* Number awaiting processing.
* Number that could not be matched.
* Whether automatic linking is running.
* Any problem requiring user intervention.

Provide simple Pause and Resume controls.

Use plain-language explanations.

Examples:

"Cannot connect to Sonarr. Check its address and API key."

"Waiting for Shoko to finish processing files."

"Some episodes could not be matched confidently. They were skipped."

Avoid exposing internal exception classes, state-machine names, or campaign identifiers in the normal interface.

Technical details should remain accessible through an advanced diagnostics section.

Keep the dashboard private by default. Do not introduce an unauthenticated Internet-facing management endpoint.

Avoid a large frontend framework unless a lightweight implementation demonstrably cannot meet the requirements.

## 7. Simplify the CLI

Keep advanced commands for development, support, troubleshooting, and backward compatibility.

The ordinary user should only encounter straightforward operations equivalent to:

* Start automatic linking.
* Check status.
* Pause linking.
* Resume linking.

The container should start automatically without an explicit CLI command.

Move specialized operations into an advanced/help surface.

Do not delete existing functionality merely to reduce the command count.

## 8. Rewrite the documentation

The README should explain the product from the user's perspective.

Its opening section should describe what the application does, not its internal implementation milestones.

The primary setup instructions should be short:

1. Install the container.
2. Enter Sonarr and Shoko connection details.
3. Start the container.
4. Automatic linking begins when safety checks pass.

Document exceptional setup requirements separately.

Move detailed material covering campaigns, attestation records, fingerprints, mapping generation, SQLite recovery, and command-line internals to advanced documentation.

Provide a practical Unraid installation example.

## 9. Preserve compatibility

Existing installations must retain their SQLite state, previous submissions, completed links, and unresolved-send history.

Do not perform a destructive state reset.

Existing advanced configurations should continue to function where possible.

Provide a clear migration strategy if the simplified configuration or automatic-linking policy requires new persistent settings.

Do not silently reinterpret old operator approvals as consent for broader automatic linking.

## 10. Validation

Prioritize end-to-end behavior rather than expanding the existing test infrastructure.

The central acceptance test should simulate a fresh installation with only the required connection settings.

Verify that the application can:

* Connect to mocked Sonarr and Shoko services.
* Identify anime files.
* Determine safe file correspondence.
* Download mappings.
* Select eligible unrecognized files.
* Process and verify links automatically when safety is established.
* Continue processing after restart.
* Skip ambiguous files without blocking unrelated work.
* Refuse unsafe Shoko configurations.
* Preserve previous links.
* Avoid replaying uncertain requests.

Also test the exceptional onboarding paths for ambiguous folders, invalid API keys, unsupported Shoko versions, and unverifiable safety settings.

Run the project's existing configured checks and Docker smoke tests.

Use proportional testing. Do not spend most of the milestone building another elaborate test framework.

## 11. Implementation approach

Implement this as an incremental product redesign.

Reuse the established matching, mapping, SQLite, and guarded-linking implementation.

Avoid rewriting the core engine.

Prioritize the following milestones:

**M1 — Zero-configuration startup:** Environment-based settings, automatic daemon startup, friendly errors, and Unraid installation support.

**M2 — Automatic environment discovery:** Anime scope detection, path mapping, and compatibility checks.

**M3 — Simplified safety model:** Automatic runtime checks and one-time human confirmation only when truly necessary.

**M4 — Automatic backlog processing:** Internal queue/campaign management, automatic verification, and sensible throughput.

**M5 — Simple status interface:** Lightweight dashboard, pause/resume controls, and actionable error reporting.

**M6 — Documentation, migration, and final integration:** Beginner-focused README, backward compatibility, end-to-end testing, and independent review.

The primary acceptance criterion is not the number of tests or CLI capabilities.

**It is whether an ordinary user can install the application, enter their API keys, and have it work safely in the background without learning its internal architecture.**

Do not request routine approval between implementation milestones.

Do not perform live Sonarr/Shoko writes, production deployment, or GitHub pushes without authorization.
