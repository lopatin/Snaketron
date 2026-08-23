export type UpdateSkinRequest =
  | {
      document: unknown;
      expectedHeadRevision: number;
      name?: string;
      priceBux?: number;
    }
  | {
      document?: never;
      expectedHeadRevision?: never;
      name?: string;
      priceBux?: number;
    };

/** Keep the revision authority beside document bytes at the wire boundary. */
export function exactSkinUpdate(request: UpdateSkinRequest): UpdateSkinRequest {
  return request;
}

/** The server opens review for immutable bytes, never for a moving head. */
export function exactPublicationRequest(revision: number, contentRef: string) {
  return { revision, contentRef };
}

export type AdminSkinDecision =
  | {
      decision: 'publish' | 'reject';
      revision: number;
      contentRef: string;
      reason?: string;
    }
  | {
      decision: 'setPublication';
      publication: 'unpublished' | 'disabled' | 'private';
      revision?: never;
      contentRef?: never;
      reason?: string;
    };

/** Review decisions bind the bytes inspected; state-only moderation does not. */
export function exactAdminSkinDecision(decision: AdminSkinDecision): AdminSkinDecision {
  return decision;
}
