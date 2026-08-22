/**
 * Keeping a skin document's texture list in step with what its layers name.
 *
 * A texture reference in SkinDoc v2 is two halves: a layer names a texture,
 * and the document's `textures` list says what that name points at. Only the
 * first half has a control in the Builder, so the second is derived — here,
 * from the layer stack, rather than by reacting to any one edit.
 */

export interface BuiltinTexture {
  id: string;
  label: string;
  kind: string;
  contentRef: string;
}

/** A declaration as it goes over the wire: `ref`, not `contentRef`. */
export interface TextureDeclaration extends Record<string, unknown> {
  name: string;
  ref: string;
  kind: string;
}

type Document = Record<string, unknown>;

/** Every texture name any layer in the stack points at, nesting included. */
export const namedTextures = (doc: Document): Set<string> => {
  const named = new Set<string>();
  const walk = (layers: unknown) => {
    if (!Array.isArray(layers)) {
      return;
    }
    for (const layer of layers as Array<Record<string, unknown>>) {
      const source = layer?.source as Record<string, unknown> | undefined;
      const texture = source?.texture;
      if (typeof texture === 'string' && texture) {
        named.add(texture);
      }
      walk(layer?.layers);
    }
  };
  walk(doc.layers);
  return named;
};

/**
 * Make the document declare exactly the textures its layers name.
 *
 * Reconciling the whole stack, rather than patching one field, is what makes
 * this reliable: a texture can arrive by adding a layer, by switching a
 * source's kind, or by picking one from the popover, and those are three
 * different paths through the document.
 *
 * It drops as well as adds, and the dropping is not housekeeping. A skin may
 * declare four textures; an append-only version of this left every texture the
 * author had *tried* sitting in the list, so browsing a fifth failed with "at
 * most 4 textures per skin" while exactly one was selected. An entry no layer
 * names is not a texture the skin has, it is one the skin had.
 *
 * Names that survive keep their existing declaration untouched, so generated
 * and uploaded art — which is in no catalogue and could not be rebuilt from a
 * name alone — lives exactly as long as something points at it.
 */
export const reconcileTextures = (doc: Document, catalogue: BuiltinTexture[]): Document => {
  const named = namedTextures(doc);
  const held = Array.isArray(doc.textures)
    ? (doc.textures as Array<Record<string, unknown>>)
    : [];

  const kept = held.filter((each) => named.has(String(each?.name)));
  const missing = [...named]
    .filter((name) => !kept.some((each) => each?.name === name))
    .map((name) => catalogue.find((art) => art.id === name))
    .filter((art): art is BuiltinTexture => Boolean(art))
    .map((art) => ({ name: art.id, ref: art.contentRef, kind: art.kind }));

  const next = [...kept, ...missing];
  // Same object when nothing moved, so an unrelated keystroke elsewhere in the
  // panel doesn't read as a document change.
  const settled = next.length === held.length && next.every((each, at) => each === held[at]);
  return settled ? doc : { ...doc, textures: next };
};
