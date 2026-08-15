import assert from 'node:assert/strict';
import test from 'node:test';
import { loadScenarioSprite } from '../../utils/scenarioAssets.ts';

type ImageEventName = 'load' | 'error';

class EventImage {
  src = '';

  complete = false;

  naturalWidth = 0;

  decode?: () => Promise<void>;

  private readonly listeners = new Map<ImageEventName, Set<() => void>>();

  addEventListener(type: ImageEventName, listener: () => void): void {
    const listeners = this.listeners.get(type) ?? new Set<() => void>();
    listeners.add(listener);
    this.listeners.set(type, listeners);
  }

  removeEventListener(type: ImageEventName, listener: () => void): void {
    this.listeners.get(type)?.delete(listener);
  }

  emit(type: ImageEventName): void {
    for (const listener of this.listeners.get(type) ?? []) {
      listener();
    }
  }

  listenerCount(): number {
    return [...this.listeners.values()].reduce((total, listeners) => (
      total + listeners.size
    ), 0);
  }
}

const asImage = (image: EventImage): HTMLImageElement => (
  image as unknown as HTMLImageElement
);

test('scenario sprite decode is mandatory when the browser supports it', async () => {
  const decodeFailure = new Error('corrupt image bytes');
  const image = new EventImage();
  image.decode = async () => {
    throw decodeFailure;
  };

  await assert.rejects(
    loadScenarioSprite(asImage(image), 'images/crash-explosion.png'),
    (error: unknown) => (
      error instanceof Error
      && error.message === 'Replay sprite failed to decode: images/crash-explosion.png'
    ),
  );
  assert.equal(image.src, 'images/crash-explosion.png');
});

test('scenario sprite falls back to load/error events when decode is absent', async () => {
  const loaded = new EventImage();
  const loadedPromise = loadScenarioSprite(asImage(loaded), 'sprite.png');
  loaded.naturalWidth = 256;
  loaded.emit('load');
  await loadedPromise;
  assert.equal(loaded.listenerCount(), 0);

  const failed = new EventImage();
  const failedPromise = loadScenarioSprite(asImage(failed), 'broken.png');
  failed.emit('error');
  await assert.rejects(failedPromise, /Replay sprite failed to load: broken\.png/);
  assert.equal(failed.listenerCount(), 0);
});

test('scenario sprite fallback handles cached success and failure', async () => {
  const cached = new EventImage();
  cached.complete = true;
  cached.naturalWidth = 128;
  await loadScenarioSprite(asImage(cached), 'cached.png');
  assert.equal(cached.listenerCount(), 0);

  const brokenCached = new EventImage();
  brokenCached.complete = true;
  await assert.rejects(
    loadScenarioSprite(asImage(brokenCached), 'cached-broken.png'),
    /Replay sprite failed to load: cached-broken\.png/,
  );
  assert.equal(brokenCached.listenerCount(), 0);
});
