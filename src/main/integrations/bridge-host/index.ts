import { Socket } from "net";
import { EventEmitter } from "events";
import { PlayerState, VideoState } from "../../player-state-store";
import IIntegration from "../integration";
import log from "electron-log";

type StateReader = () => PlayerState;
type CommandExecutor = (command: string, data: Record<string, unknown>) => Promise<void>;

const SOCKET_PATH = process.env.YTM_BRIDGE_SOCKET || "/tmp/ytm-bridge.sock";

function canonicalState(state: PlayerState) {
  const details = state.videoDetails;
  const track = details
    ? {
        video_id: details.id,
        title: details.title,
        artists: details.author ? [details.author] : [],
        album: details.album,
        album_id: details.albumId,
        duration_seconds: details.durationSeconds,
        thumbnail_url: details.thumbnails?.[0]?.url ?? null,
        source: "ytmdesktop"
      }
    : null;
  const queue = state.queue
    ? {
        tracks: (state.queue.items ?? []).map(item => ({
          video_id: item.videoId,
          title: item.title,
          artists: item.author ? [item.author] : [],
          duration_seconds: null,
          thumbnail_url: item.thumbnails?.[0]?.url ?? null,
          source: "ytmdesktop"
        })),
        current_index: state.queue.selectedItemIndex,
        repeat: ["none", "all", "one"][state.queue.repeatMode] ?? "none",
        shuffle: false,
        source: "ytmdesktop"
      }
    : null;
  return {
    status: state.trackState === VideoState.Playing ? "playing" : state.trackState === VideoState.Paused ? "paused" : "buffering",
    track,
    position_seconds: state.videoProgress,
    duration_seconds: details?.durationSeconds ?? null,
    volume: state.volume,
    muted: state.muted,
    queue,
    provider: "ytmdesktop"
  };
}

export default class BridgeHost implements IIntegration {
  private socket: Socket | null = null;
  private buffer = "";
  private stateReader: StateReader | null = null;
  private commandExecutor: CommandExecutor | null = null;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private stateChanged = new EventEmitter();

  public provide(stateReader: StateReader, commandExecutor: CommandExecutor): void {
    this.stateReader = stateReader;
    this.commandExecutor = commandExecutor;
    this.stateChanged.on("changed", this.sendState);
  }

  public async enable(): Promise<void> {
    this.connect();
  }

  public async disable(): Promise<void> {
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    this.socket?.destroy();
    this.socket = null;
  }

  public notifyStateChanged(): void {
    this.stateChanged.emit("changed");
  }

  private connect(): void {
    if (this.socket || !this.stateReader) return;
    const socket = new Socket();
    this.socket = socket;
    socket.on("connect", () => {
      this.send({
        type: "provider_register",
        capabilities: {
          provider: "ytmdesktop",
          operations: ["play", "pause", "next", "previous", "seek", "state", "like", "dislike"],
          connected: true,
          detail: "Electron YouTube Music host"
        },
        state: canonicalState(this.stateReader!())
      });
    });
    socket.on("data", data => this.receive(data.toString("utf8")));
    socket.on("error", error => log.debug("YTM bridge host unavailable", error.message));
    socket.on("close", () => {
      this.socket = null;
      this.reconnectTimer = setTimeout(() => this.connect(), 2000);
    });
    socket.connect(SOCKET_PATH);
  }

  private receive(chunk: string): void {
    this.buffer += chunk;
    let newline = this.buffer.indexOf("\n");
    while (newline >= 0) {
      const line = this.buffer.slice(0, newline);
      this.buffer = this.buffer.slice(newline + 1);
      newline = this.buffer.indexOf("\n");
      if (!line) continue;
      try {
        const message = JSON.parse(line);
        if (message.type === "provider_command") this.handleCommand(message);
      } catch (error) {
        log.warn("Invalid YTM bridge message", error);
      }
    }
  }

  private async handleCommand(message: { request_id: string; command: string; data?: Record<string, unknown> }): Promise<void> {
    try {
      await this.commandExecutor?.(message.command, message.data ?? {});
      this.send({
        type: "provider_response",
        request_id: message.request_id,
        ok: true,
        state: canonicalState(this.stateReader!())
      });
    } catch (error) {
      this.send({
        type: "provider_response",
        request_id: message.request_id,
        ok: false,
        error: { code: "command_failed", message: String(error) }
      });
    }
  }

  private sendState = (): void => {
    if (this.socket?.writable && this.stateReader) {
      this.send({ type: "provider_event", event: "playback_state", data: canonicalState(this.stateReader()) });
    }
  };

  private send(message: Record<string, unknown>): void {
    if (this.socket?.writable) this.socket.write(`${JSON.stringify(message)}\n`);
  }
}
