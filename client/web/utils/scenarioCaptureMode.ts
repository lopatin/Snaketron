const SCENARIO_CAPTURE_PATH = '/qa/scenario-player';
const PLAY_OF_THE_GAME_QA_PATH = '/qa/play-of-the-game';

const routeUrls = (): URL[] => {
  if (typeof window === 'undefined') {
    return [];
  }

  const urls = [new URL(window.location.href)];
  const hashRoute = window.location.hash.replace(/^#/, '');
  if (hashRoute.startsWith('/')) {
    urls.push(new URL(hashRoute, window.location.origin));
  }
  return urls;
};

export const isScenarioCaptureMode = (): boolean => (
  process.env.NODE_ENV !== 'production' &&
  routeUrls().some((url) => (
    url.pathname === SCENARIO_CAPTURE_PATH &&
    url.searchParams.get('capture') === '1'
  ))
);

export const isScenarioPlayerQaRoute = (): boolean => (
  process.env.NODE_ENV !== 'production' &&
  routeUrls().some((url) => url.pathname === SCENARIO_CAPTURE_PATH)
);

export const isPlayOfTheGameQaRoute = (): boolean => (
  process.env.NODE_ENV !== 'production' &&
  routeUrls().some((url) => url.pathname === PLAY_OF_THE_GAME_QA_PATH)
);

/**
 * Capture must be deterministic even when a developer has no API or socket
 * running. App.tsx also bypasses every provider in this mode; these immediate
 * stubs are a second boundary against future module-level or route-level
 * networking accidentally making the capture readiness promise hang.
 */
export const installScenarioCaptureNetworkStubs = (): void => {
  if (!isScenarioCaptureMode() || window.__SNAKETRON_CAPTURE_NETWORK_STUBS__) {
    return;
  }

  window.__SNAKETRON_CAPTURE_NETWORK_STUBS__ = true;
  document.documentElement.dataset.scenarioCapture = 'true';

  // Canvas text otherwise resolves to CoreText Arial on macOS and a
  // fontconfig substitute in Docker. Register capture-only author fonts under
  // the names already used by the production renderer, then include their
  // explicit load promise in ScenarioCanvas.ready(). Dynamic FontFace entries
  // exist only in this capture document, so normal gameplay typography is not
  // changed.
  const captureFonts = [
    new FontFace(
      'Snaketron Capture Sans',
      'url("/capture-fonts/Inter-Variable.ttf")',
      { style: 'normal', weight: '100 900' },
    ),
    new FontFace(
      'Snaketron Capture Black',
      'url("/capture-fonts/BarlowCondensed-ExtraBoldItalic.ttf")',
      { style: 'italic', weight: '800' },
    ),
  ];
  captureFonts.forEach((font) => document.fonts.add(font));
  window.__SNAKETRON_CAPTURE_FONTS_READY__ = Promise.all(
    captureFonts.map((font) => font.load()),
  ).then(() => undefined);

  const nativeFetch = window.fetch.bind(window);
  window.fetch = (async (input, init) => {
    const requestUrl = new URL(
      input instanceof Request ? input.url : String(input),
      window.location.href,
    );
    const isLocalAsset = requestUrl.origin === window.location.origin &&
      !requestUrl.pathname.startsWith('/api/');
    if (isLocalAsset) {
      return nativeFetch(input, init);
    }
    return new Response(
      JSON.stringify({ offline: true, capture: true }),
      {
        status: 503,
        headers: { 'content-type': 'application/json' },
      },
    );
  }) as typeof window.fetch;

  class CaptureWebSocket extends EventTarget {
    static readonly CONNECTING = WebSocket.CONNECTING;

    static readonly OPEN = WebSocket.OPEN;

    static readonly CLOSING = WebSocket.CLOSING;

    static readonly CLOSED = WebSocket.CLOSED;

    readonly CONNECTING = CaptureWebSocket.CONNECTING;

    readonly OPEN = CaptureWebSocket.OPEN;

    readonly CLOSING = CaptureWebSocket.CLOSING;

    readonly CLOSED = CaptureWebSocket.CLOSED;

    readonly url: string;

    readonly protocol = '';

    readonly extensions = '';

    readonly bufferedAmount = 0;

    binaryType: BinaryType = 'blob';

    readyState: number = CaptureWebSocket.CONNECTING;

    onopen: ((this: WebSocket, event: Event) => unknown) | null = null;

    onclose: ((this: WebSocket, event: CloseEvent) => unknown) | null = null;

    onerror: ((this: WebSocket, event: Event) => unknown) | null = null;

    onmessage: ((this: WebSocket, event: MessageEvent) => unknown) | null = null;

    constructor(url: string | URL) {
      super();
      this.url = String(url);
      queueMicrotask(() => {
        if (this.readyState !== CaptureWebSocket.CONNECTING) {
          return;
        }
        this.readyState = CaptureWebSocket.OPEN;
        const event = new Event('open');
        this.dispatchEvent(event);
        this.onopen?.call(this as unknown as WebSocket, event);
      });
    }

    send(_data: string | ArrayBufferLike | Blob | ArrayBufferView): void {}

    close(_code?: number, _reason?: string): void {
      if (this.readyState === CaptureWebSocket.CLOSED) {
        return;
      }
      this.readyState = CaptureWebSocket.CLOSED;
      const event = new CloseEvent('close', {
        code: 1000,
        reason: 'scenario capture network stub',
        wasClean: true,
      });
      this.dispatchEvent(event);
      this.onclose?.call(this as unknown as WebSocket, event);
    }
  }

  window.WebSocket = CaptureWebSocket as unknown as typeof WebSocket;
};

declare global {
  interface Window {
    __SNAKETRON_CAPTURE_NETWORK_STUBS__?: boolean;
    __SNAKETRON_CAPTURE_FONTS_READY__?: Promise<void>;
  }
}
