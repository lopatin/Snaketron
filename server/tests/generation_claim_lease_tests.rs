//! A generation job survives the worker that was holding it.
//!
//! PRD section 20.2 recorded this as the one prerequisite a texture worker
//! needs before it can be written: a claim used to set `generating` with no
//! expiry *and* remove the job's queue entry, so a worker that died mid-job
//! took the work out of the only index that could find it again. The state said
//! `generating`, nothing would ever say otherwise, and the client polled a
//! value that could not change.
//!
//! These run against LocalStack because the behaviour being tested *is* the
//! conditional write: a unit test over a fake would be asserting that the test
//! double does what the real condition expression is supposed to do.

use anyhow::{Context, Result};
use server::db::{Database, dynamodb::DynamoDatabase};
use server::generation::{GenerationJob, JobState, LEASE_MS, Spend};
use server::texture::TextureKind;
use uuid::Uuid;

fn queued_job(id: &str, created_at_ms: i64) -> GenerationJob {
    GenerationJob {
        job_id: id.to_string(),
        owner_user_id: 4_242,
        kind: TextureKind::Coat,
        prompt: "a lease test".to_string(),
        state: JobState::Queued,
        spend: Spend::default(),
        texture_id: None,
        failure: None,
        detail: None,
        subject: Some("a lease test".to_string()),
        source_ref: None,
        reference_refs: Vec::new(),
        created_at_ms,
        updated_at_ms: created_at_ms,
        lease_until_ms: None,
    }
}

/// Start `./test-deps.sh` (LocalStack) before running this.
#[tokio::test]
#[ignore = "requires LocalStack DynamoDB"]
async fn a_job_whose_worker_died_is_claimed_again_and_one_in_flight_is_not() -> Result<()> {
    let unique = Uuid::new_v4().simple().to_string();
    // SAFETY: this binary holds one test, so no sibling can observe the value.
    unsafe {
        std::env::set_var("DYNAMODB_TABLE_PREFIX", format!("genlease_{unique}"));
    }
    let db = DynamoDatabase::new().await?;

    let now = chrono::Utc::now().timestamp_millis();
    let job = queued_job(&format!("lease-{}", &unique[..16]), now);
    db.create_generation_job(&job).await?;

    // One worker takes it, and takes a lease with it.
    let first = db
        .claim_generation_job("worker-a", now)
        .await?
        .context("a queued job is claimable")?;
    assert_eq!(first.job_id, job.job_id);
    assert_eq!(first.state, JobState::Generating);
    assert_eq!(
        first.lease_until_ms,
        Some(now + LEASE_MS),
        "a claim has to record when it lapses, or nothing can tell a slow \
         worker from a dead one"
    );

    // A second worker arriving while the lease holds must not get it.
    assert!(
        db.claim_generation_job("worker-b", now + 1_000)
            .await?
            .is_none(),
        "two workers took the same job, which is two bills for one texture"
    );

    // The first worker writes progress. This is a whole-item rewrite, and the
    // lease has to survive it — a lease that only the claim wrote would be
    // erased here, which is the same shape as the bug that once made a job's
    // lifetime disappear on its first update.
    let mut working = first.clone();
    working.state = JobState::Repairing;
    working.spend = Spend {
        provider_calls: 1,
        usd_micros: 4_000,
    };
    working.updated_at_ms = now + 2_000;
    db.update_generation_job(&working).await?;

    let read_back = db
        .get_generation_job(&job.job_id)
        .await?
        .context("the job is still there")?;
    assert_eq!(read_back.lease_until_ms, Some(now + LEASE_MS));
    assert!(
        db.claim_generation_job("worker-c", now + 3_000)
            .await?
            .is_none(),
        "a job being actively worked was handed to someone else"
    );

    // Now the worker dies. Nothing writes, the lease lapses, and the job has to
    // become somebody else's problem rather than nobody's.
    let after_lease = now + LEASE_MS + 1;
    let rescued = db
        .claim_generation_job("worker-d", after_lease)
        .await?
        .context("a lapsed lease has to be reclaimable, or the job is lost")?;
    assert_eq!(rescued.job_id, job.job_id);
    assert_eq!(rescued.state, JobState::Generating);
    assert_eq!(rescued.lease_until_ms, Some(after_lease + LEASE_MS));

    // Finishing takes it out of the queue for good.
    let mut done = rescued.clone();
    done.state = JobState::Done;
    done.texture_id = Some(77);
    done.updated_at_ms = after_lease + 10;
    db.update_generation_job(&done).await?;

    assert!(
        db.claim_generation_job("worker-e", after_lease + 10 * LEASE_MS)
            .await?
            .is_none(),
        "a finished job came back out of the queue"
    );

    Ok(())
}
