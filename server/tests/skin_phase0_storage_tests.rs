//! Transactional skin-foundation checks against LocalStack DynamoDB.
//!
//! Start `./test-deps.sh`, then run
//! `cargo test -p server --test skin_phase0_storage_tests -- --ignored`.

use anyhow::{Context, Result};
use server::db::{Database, dynamodb::DynamoDatabase};
use server::skin_store::{
    NewRevision, NewSkin, Publication, SkinKind, SkinNamespace, SkinReviewDecision, SkinWriteError,
};
use uuid::Uuid;

fn revision<'a>(document: &'a str, content_ref: &'a str) -> NewRevision<'a> {
    NewRevision {
        document,
        content_ref,
        texture_refs: &[],
        validated_schema: 2,
        contains_text: false,
    }
}

/// Revision append and final review are the two writes the factory must be
/// able to retry after an unknown network outcome. This test proves both
/// convergence paths through the real DynamoDB transaction implementation.
#[tokio::test]
#[ignore = "requires LocalStack DynamoDB"]
async fn append_and_exact_publish_are_atomic_idempotent_and_conflict_safe() -> Result<()> {
    let unique = Uuid::new_v4().simple().to_string();
    // SAFETY: this integration-test binary contains one test, so no peer can
    // observe the temporary table namespace.
    unsafe {
        std::env::set_var("DYNAMODB_TABLE_PREFIX", format!("skinphase0_{unique}"));
    }
    let db = DynamoDatabase::new().await?;
    let document_one = r#"{"schema_version":2,"value":"one"}"#;
    let ref_one = skin_schema::content::reference_for_bytes(document_one.as_bytes());
    let skin = db
        .create_skin(NewSkin {
            creator_user_id: 81_810,
            creator_username: Some("factory"),
            kind: SkinKind::Snake,
            namespace: SkinNamespace::Production,
            name: "Atomic",
            revision: revision(document_one, &ref_one),
            idempotency_key: Some("concept-atomic"),
            request_hash: Some(&ref_one),
        })
        .await?;
    let create_retry = db
        .create_skin(NewSkin {
            creator_user_id: 81_810,
            creator_username: Some("factory"),
            kind: SkinKind::Snake,
            namespace: SkinNamespace::Production,
            name: "Atomic",
            revision: revision(document_one, &ref_one),
            idempotency_key: Some("concept-atomic"),
            request_hash: Some(&ref_one),
        })
        .await?;
    assert_eq!(create_retry.skin_id, skin.skin_id);
    let reused = db
        .create_skin(NewSkin {
            creator_user_id: 81_810,
            creator_username: Some("factory"),
            kind: SkinKind::Snake,
            namespace: SkinNamespace::Production,
            name: "Different",
            revision: revision(document_one, &ref_one),
            idempotency_key: Some("concept-atomic"),
            request_hash: Some("sha256:different-request"),
        })
        .await
        .expect_err("one key cannot name a different create payload");
    assert!(reused.downcast_ref::<SkinWriteError>().is_some());

    let document_two = r#"{"schema_version":2,"value":"two"}"#;
    let ref_two = skin_schema::content::reference_for_bytes(document_two.as_bytes());
    let appended = db
        .put_skin_revision(skin.skin_id, 1, revision(document_two, &ref_two))
        .await?;
    assert_eq!(
        (appended.head_revision, appended.head_content_ref.as_str()),
        (2, ref_two.as_str())
    );

    // Exact response-loss retry converges to the already-created immutable
    // revision instead of creating revision 3.
    let retried = db
        .put_skin_revision(skin.skin_id, 1, revision(document_two, &ref_two))
        .await?;
    assert_eq!(retried.head_revision, 2);
    assert!(db.get_skin_revision(skin.skin_id, 3).await?.is_none());

    // A different stale write is a typed conflict and still creates nothing.
    let document_other = r#"{"schema_version":2,"value":"other"}"#;
    let ref_other = skin_schema::content::reference_for_bytes(document_other.as_bytes());
    let error = db
        .put_skin_revision(skin.skin_id, 1, revision(document_other, &ref_other))
        .await
        .expect_err("stale head must conflict");
    assert!(error.downcast_ref::<SkinWriteError>().is_some());
    assert!(db.get_skin_revision(skin.skin_id, 3).await?.is_none());

    db.set_skin_pending_revision(skin.skin_id, Some(2)).await?;
    let error = db
        .set_skin_pending_revision(skin.skin_id, Some(1))
        .await
        .expect_err("a second immutable revision must not replace an open review");
    assert!(error.downcast_ref::<SkinWriteError>().is_some());
    assert_eq!(
        db.get_skin(skin.skin_id)
            .await?
            .context("skin exists")?
            .pending_revision,
        Some(2)
    );
    let error = db
        .clear_skin_pending_revision_exact(skin.skin_id, 1)
        .await
        .expect_err("a stale rejection must not clear revision 2");
    assert!(error.downcast_ref::<SkinWriteError>().is_some());
    assert_eq!(
        db.get_skin(skin.skin_id)
            .await?
            .context("skin exists")?
            .pending_revision,
        Some(2)
    );
    db.clear_skin_pending_revision_exact(skin.skin_id, 2)
        .await?;
    // Response-loss retry converges without requiring an open marker.
    db.clear_skin_pending_revision_exact(skin.skin_id, 2)
        .await?;
    assert_eq!(
        db.get_skin(skin.skin_id)
            .await?
            .context("skin exists")?
            .pending_revision,
        None
    );
    db.set_skin_pending_revision(skin.skin_id, Some(2)).await?;
    let wrong = format!("sha256:{}", "f".repeat(64));
    let error = db
        .decide_skin_review(
            skin.skin_id,
            SkinReviewDecision::Publish,
            Some(2),
            Some(&wrong),
            99,
            Some("wrong hash test"),
        )
        .await
        .expect_err("a different hash must not publish");
    assert!(error.downcast_ref::<SkinWriteError>().is_some());
    let unchanged = db.get_skin(skin.skin_id).await?.context("skin exists")?;
    assert_eq!(unchanged.publication, Publication::Private);
    assert_eq!(unchanged.pending_revision, Some(2));

    db.decide_skin_review(
        skin.skin_id,
        SkinReviewDecision::Publish,
        Some(2),
        Some(&ref_two),
        99,
        Some("approved"),
    )
    .await?;
    // Unknown-outcome retry is a no-op success.
    db.decide_skin_review(
        skin.skin_id,
        SkinReviewDecision::Publish,
        Some(2),
        Some(&ref_two),
        99,
        Some("approved"),
    )
    .await?;

    let published = db.get_skin(skin.skin_id).await?.context("skin exists")?;
    assert_eq!(published.publication, Publication::Published);
    assert_eq!(published.published_revision, Some(2));
    assert_eq!(
        published.published_content_ref.as_deref(),
        Some(ref_two.as_str())
    );
    assert_eq!(published.pending_revision, None);
    assert!(
        db.get_skin_revision(skin.skin_id, 2)
            .await?
            .context("revision exists")?
            .review_approved
    );

    // Rejecting an edit is an exact decision about the submitted bytes, but
    // it is not a publication transition: revision 2 remains live throughout.
    let document_three = r#"{"schema_version":2,"value":"three"}"#;
    let ref_three = skin_schema::content::reference_for_bytes(document_three.as_bytes());
    let edited = db
        .put_skin_revision(skin.skin_id, 2, revision(document_three, &ref_three))
        .await?;
    assert_eq!(edited.head_revision, 3);
    db.set_skin_pending_revision(skin.skin_id, Some(3)).await?;
    let wrong_reject = db
        .decide_skin_review(
            skin.skin_id,
            SkinReviewDecision::Reject,
            Some(3),
            Some(&wrong),
            99,
            Some("moving target must survive"),
        )
        .await
        .expect_err("a wrong hash must not clear the pending edit");
    assert!(wrong_reject.downcast_ref::<SkinWriteError>().is_some());
    assert_eq!(
        db.get_skin(skin.skin_id)
            .await?
            .context("skin exists")?
            .pending_revision,
        Some(3)
    );
    db.decide_skin_review(
        skin.skin_id,
        SkinReviewDecision::Reject,
        Some(3),
        Some(&ref_three),
        99,
        Some("not ready"),
    )
    .await?;
    // Unknown-outcome retry is harmless and cannot reach a newer request.
    db.decide_skin_review(
        skin.skin_id,
        SkinReviewDecision::Reject,
        Some(3),
        Some(&ref_three),
        99,
        Some("not ready"),
    )
    .await?;
    let rejected_edit = db.get_skin(skin.skin_id).await?.context("skin exists")?;
    assert_eq!(rejected_edit.publication, Publication::Published);
    assert_eq!(rejected_edit.published_revision, Some(2));
    assert_eq!(
        rejected_edit.published_content_ref.as_deref(),
        Some(ref_two.as_str())
    );
    assert_eq!(rejected_edit.pending_revision, None);
    assert_eq!(rejected_edit.head_revision, 3);
    let rejected_revision = db
        .get_skin_revision(skin.skin_id, 3)
        .await?
        .context("rejected revision exists")?;
    assert!(rejected_revision.review_rejected);
    assert!(!rejected_revision.review_approved);

    // A late response-loss retry proves only the exact revision decision. It
    // must not restore the publication snapshot captured before a separate
    // moderator took the already-published skin down.
    db.decide_skin_review(
        skin.skin_id,
        SkinReviewDecision::SetPublication(Publication::Disabled),
        None,
        None,
        100,
        Some("separate takedown"),
    )
    .await?;
    db.decide_skin_review(
        skin.skin_id,
        SkinReviewDecision::Reject,
        Some(3),
        Some(&ref_three),
        99,
        Some("not ready"),
    )
    .await?;
    assert_eq!(
        db.get_skin(skin.skin_id)
            .await?
            .context("skin exists")?
            .publication,
        Publication::Disabled,
        "a rejection retry cannot resurrect a later moderation state"
    );

    // Identical document bytes under another skin return a complete,
    // deterministic equivalence class rather than one arbitrary GSI row.
    let twin = db
        .create_skin(NewSkin {
            creator_user_id: 81_811,
            creator_username: Some("factory-two"),
            kind: SkinKind::Snake,
            namespace: SkinNamespace::Production,
            name: "Twin",
            revision: revision(document_two, &ref_two),
            idempotency_key: Some("concept-twin"),
            request_hash: Some(&ref_two),
        })
        .await?;
    let resolved = db.resolve_content_ref(&ref_two).await?;
    let identities: Vec<(i32, u32)> = resolved
        .iter()
        .map(|(skin, revision)| (skin.skin_id, revision.revision))
        .collect();
    assert!(identities.contains(&(skin.skin_id, 2)));
    assert!(identities.contains(&(twin.skin_id, 1)));
    assert!(identities.windows(2).all(|pair| pair[0] <= pair[1]));

    db.set_skin_pending_revision(twin.skin_id, Some(1)).await?;
    db.decide_skin_review(
        twin.skin_id,
        SkinReviewDecision::Reject,
        Some(1),
        Some(&ref_two),
        99,
        Some("new draft rejected"),
    )
    .await?;
    let rejected_draft = db.get_skin(twin.skin_id).await?.context("twin exists")?;
    assert_eq!(rejected_draft.publication, Publication::Private);
    assert_eq!(rejected_draft.pending_revision, None);

    // A reject arriving after another admin published the same exact target
    // must report a conflict, not mistake "published + no pending slot" for
    // its own lost response.
    let race = db
        .create_skin(NewSkin {
            creator_user_id: 81_812,
            creator_username: Some("factory-race"),
            kind: SkinKind::Snake,
            namespace: SkinNamespace::Production,
            name: "Review race",
            revision: revision(document_two, &ref_two),
            idempotency_key: Some("concept-review-race"),
            request_hash: Some(&ref_two),
        })
        .await?;
    db.set_skin_pending_revision(race.skin_id, Some(1)).await?;
    db.decide_skin_review(
        race.skin_id,
        SkinReviewDecision::Publish,
        Some(1),
        Some(&ref_two),
        99,
        Some("race winner"),
    )
    .await?;
    let late_reject = db
        .decide_skin_review(
            race.skin_id,
            SkinReviewDecision::Reject,
            Some(1),
            Some(&ref_two),
            100,
            Some("race loser"),
        )
        .await
        .expect_err("a published target cannot converge as a rejection");
    assert!(late_reject.downcast_ref::<SkinWriteError>().is_some());
    let race_winner = db.get_skin(race.skin_id).await?.context("race exists")?;
    assert_eq!(race_winner.publication, Publication::Published);
    assert_eq!(race_winner.published_revision, Some(1));
    let winning_revision = db
        .get_skin_revision(race.skin_id, 1)
        .await?
        .context("winning revision exists")?;
    assert!(winning_revision.review_approved);
    assert!(!winning_revision.review_rejected);

    let evaluation = db
        .create_skin(NewSkin {
            creator_user_id: 81_810,
            creator_username: Some("factory"),
            kind: SkinKind::Snake,
            namespace: SkinNamespace::Evaluation,
            name: "Evaluation only",
            revision: revision(document_two, &ref_two),
            idempotency_key: Some("factory-trial:isolated"),
            request_hash: Some(&ref_two),
        })
        .await?;
    db.set_skin_pending_revision(evaluation.skin_id, Some(1))
        .await?;
    let error = db
        .decide_skin_review(
            evaluation.skin_id,
            SkinReviewDecision::Publish,
            Some(1),
            Some(&ref_two),
            99,
            Some("must remain isolated"),
        )
        .await
        .expect_err("evaluation namespace must never publish");
    assert!(matches!(
        error.downcast_ref::<SkinWriteError>(),
        Some(SkinWriteError::EvaluationOnly)
    ));
    Ok(())
}
