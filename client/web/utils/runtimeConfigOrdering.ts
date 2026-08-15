interface RuntimeConfigResponseOrder {
  requestSequence: number;
  latestRequestSequence: number;
  responseVersion: number;
  appliedVersion: number;
}

export const shouldApplyRuntimeConfigResponse = ({
  requestSequence,
  latestRequestSequence,
  responseVersion,
  appliedVersion,
}: RuntimeConfigResponseOrder): boolean => (
  requestSequence === latestRequestSequence || responseVersion > appliedVersion
);
