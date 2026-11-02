package resourcepack

import (
	"bytes"
	"encoding/hex"
	"errors"

	"github.com/edwingeng/deque/v2"
	"github.com/robinbraemer/event"
	"go.minekube.com/common/minecraft/component"
	"go.minekube.com/gate/pkg/edition/java/proto/packet"
	"go.minekube.com/gate/pkg/edition/java/proto/version"
	"go.minekube.com/gate/pkg/gate/proto"
	"go.minekube.com/gate/pkg/util/uuid"
)

type legacyResponseState uint8

const (
	legacyResponseUnknown legacyResponseState = iota
	legacyResponseAccepted
	legacyResponseDeclined
)

type legacyQueuedPack struct {
	info    *Info
	backend proto.PacketWriter
}

// Legacy (Minecraft +1.17) ResourcePackHandler.
type legacyHandler struct {
	player   Player
	eventMgr event.Manager

	rwMutex
	prevResourceResponse legacyResponseState
	outstandingPacks     *deque.Deque[*legacyQueuedPack]
	activePack           *legacyQueuedPack
	advancing            bool
	responding           bool
	pendingPack          *Info
	appliedPack          *Info
}

// 未回答と拒否を区別し、最初のパックを必ず提示できるようにする。
func newLegacyHandler(player Player, eventMgr event.Manager) *legacyHandler {
	return &legacyHandler{
		player:               player,
		eventMgr:             eventMgr,
		prevResourceResponse: legacyResponseUnknown,
		outstandingPacks:     deque.NewDeque[*legacyQueuedPack](),
	}
}

var _ Handler = (*legacyHandler)(nil)

func (h *legacyHandler) FirstAppliedPack() *Info {
	h.RLock()
	defer h.RUnlock()
	return h.appliedPack
}

func (h *legacyHandler) FirstPendingPack() *Info {
	h.RLock()
	defer h.RUnlock()
	return h.pendingPack
}

func (h *legacyHandler) AppliedResourcePacks() []*Info {
	h.RLock()
	defer h.RUnlock()
	if h.appliedPack == nil {
		return nil
	}
	return []*Info{h.appliedPack}
}

func (h *legacyHandler) PendingResourcePacks() []*Info {
	h.RLock()
	defer h.RUnlock()
	if h.pendingPack == nil {
		return nil
	}
	return []*Info{h.pendingPack}
}

func (h *legacyHandler) ClearAppliedResourcePacks() {
	h.Lock()
	defer h.Unlock()
	// This is valid only for players with 1.20.2 versions
	h.appliedPack = nil
}

func (h *legacyHandler) Remove(id uuid.UUID) bool {
	panic("Cannot remove a ResourcePack from a legacy client")
}

func (h *legacyHandler) QueueResourcePack(info *Info) error {
	return h.queueResourcePack(info, nil)
}

// 自動拒否にはクライアントの応答パケットがないため、要求元の接続を保持する。
func (h *legacyHandler) QueueResourcePackFromBackend(info *Info, backend proto.PacketWriter) error {
	return h.queueResourcePack(info, backend)
}

// コールバックや書き込みから再入できるよう、ロックを解放してからキューを進める。
func (h *legacyHandler) queueResourcePack(info *Info, backend proto.PacketWriter) error {
	if info == nil {
		return errors.New("resource pack info is nil")
	}
	queued := &legacyQueuedPack{info: info, backend: backend}
	h.Lock()
	h.outstandingPacks.PushBack(queued)
	start := h.startQueueLocked()
	h.Unlock()
	if start {
		return h.tickResourcePackQueue(queued)
	}
	return nil
}

// 複数の呼び出し元がキューを同時に進めることを防ぐ。
func (h *legacyHandler) startQueueLocked() bool {
	if h.advancing || h.responding || h.activePack != nil || h.outstandingPacks.Len() == 0 {
		return false
	}
	h.advancing = true
	return true
}

// owner 以外の要求のエラーを呼び出し元に返すと、別のパックを誤って拒否してしまう。
func (h *legacyHandler) tickResourcePackQueue(owner *legacyQueuedPack) error {
	var result error
	for {
		supportsForced := h.player.Protocol().GreaterEqual(version.Minecraft_1_17)
		h.Lock()
		queued, ok := h.outstandingPacks.Front()
		if !ok || h.activePack != nil || h.responding {
			h.advancing = false
			h.Unlock()
			return result
		}
		decline := h.prevResourceResponse == legacyResponseDeclined &&
			(!queued.info.ShouldForce || !supportsForced)
		if decline {
			// 1.17 以降の必須パックは過去に拒否していても再度提示できる。
			h.outstandingPacks.PopFront()
			h.responding = true
			h.Unlock()
			_, err := h.dispatchResponse(queued, &ResponseBundle{
				ID:     queued.info.ID,
				Hash:   Hash(hex.EncodeToString(queued.info.Hash)),
				Status: DeclinedResponseStatus,
			}, h.shouldDisconnectForForcePack)
			h.Lock()
			h.responding = false
			h.Unlock()
			if owner == nil || owner == queued {
				result = errors.Join(result, err)
			}
			continue
		}

		// 同時に追加された要求による二重送信を防ぐため、書き込み前に先頭を予約する。
		h.activePack = queued
		h.Unlock()
		err := h.SendResourcePackRequestPacket(queued.info)
		if err == nil {
			continue
		}

		h.Lock()
		stillActive := h.activePack == queued
		if stillActive {
			h.outstandingPacks.PopFront()
			h.activePack = nil
			if h.pendingPack == queued.info {
				h.pendingPack = nil
			}
		}
		h.Unlock()
		if !stillActive {
			// 書き込み中に終端応答が届き、この要求はすでに完了している。
			continue
		}
		if owner != queued && queued.backend != nil {
			// キュー追加時の呼び出し元はすでに戻っており、この送信エラーを返せない。
			err = errors.Join(err, queued.backend.WritePacket((&ResponseBundle{
				ID:     queued.info.ID,
				Hash:   Hash(hex.EncodeToString(queued.info.Hash)),
				Status: DeclinedResponseStatus,
			}).ResponsePacket()))
		}
		if owner == nil || owner == queued {
			result = errors.Join(result, err)
		}
	}
}

func (h *legacyHandler) OnResourcePackResponse(bundle *ResponseBundle) (bool, error) {
	return h.onResourcePackResponse(bundle, h.shouldDisconnectForForcePack)
}

// 応答の配送が終わるまでキューを止め、再入時も応答順を維持する。
func (h *legacyHandler) onResourcePackResponse(
	bundle *ResponseBundle,
	shouldDisconnectForForcePack func(e *PlayerResourcePackStatusEvent) bool,
) (bool, error) {
	h.Lock()
	queued := h.activePack
	if queued == nil {
		h.Unlock()
		// 対応する要求がなければ、遅延応答の安全な転送先を特定できない。
		return true, nil
	}
	// 旧形式の応答には通常 ID がないため、存在する場合のみ照合する。
	if (bundle.ID != uuid.Nil && queued.info.ID != uuid.Nil && bundle.ID != queued.info.ID) ||
		(bundle.Status == AcceptedResponseStatus && h.pendingPack == queued.info) {
		h.Unlock()
		return true, nil
	}
	terminal := !bundle.Status.Intermediate()
	if terminal {
		h.outstandingPacks.PopFront()
		h.activePack = nil
		h.pendingPack = nil
		// コールバックから要求が追加されても、応答の書き込みが終わるまで次を送らない。
		h.responding = true
	}
	switch bundle.Status {
	case AcceptedResponseStatus:
		h.prevResourceResponse = legacyResponseAccepted
		h.pendingPack = queued.info
	case DeclinedResponseStatus:
		h.prevResourceResponse = legacyResponseDeclined
	case SuccessfulResponseStatus:
		h.appliedPack = queued.info
	case DiscardedResponseStatus:
		if queued.info.ID != uuid.Nil &&
			h.appliedPack != nil &&
			h.appliedPack.ID == queued.info.ID {
			h.appliedPack = nil
		}
	}
	h.Unlock()

	handled, err := h.dispatchResponse(queued, bundle, shouldDisconnectForForcePack)
	if terminal {
		h.Lock()
		h.responding = false
		start := h.startQueueLocked()
		h.Unlock()
		if start {
			err = errors.Join(err, h.tickResourcePackQueue(nil))
		}
	}
	return handled, err
}

// コールバックや書き込みは再入し得るため、ロックの外で実行する。
func (h *legacyHandler) dispatchResponse(
	queued *legacyQueuedPack,
	bundle *ResponseBundle,
	shouldDisconnectForForcePack func(e *PlayerResourcePackStatusEvent) bool,
) (bool, error) {
	e := newPlayerResourcePackStatusEvent(h.player, bundle.Status, bundle.ID, *queued.info)
	event.FireParallel(h.eventMgr, e, func(e *PlayerResourcePackStatusEvent) {
		if shouldDisconnectForForcePack(e) {
			h.player.Disconnect(&component.Translation{
				Key: "multiplayer.requiredTexturePrompt.disconnect",
			})
		}
	})
	if queued.backend != nil {
		return true, queued.backend.WritePacket(bundle.ResponsePacket())
	}
	return h.HandleResponseResult(queued.info, bundle)
}

func (h *legacyHandler) HasPackAppliedByHash(hash []byte) bool {
	h.RLock()
	defer h.RUnlock()
	if hash == nil {
		return false
	}
	return h.appliedPack != nil && bytes.Equal(h.appliedPack.Hash, hash)
}

func (h *legacyHandler) shouldDisconnectForForcePack(e *PlayerResourcePackStatusEvent) bool {
	return e.Status() == DeclinedResponseStatus &&
		e.PackInfo().ShouldForce
}

func (h *legacyHandler) QueueResourcePackRequest(request *packet.ResourcePackRequest) error {
	return queueResourcePackRequest(request, h)
}

func (h *legacyHandler) SendResourcePackRequestPacket(pack *Info) error {
	return sendResourcePackRequestPacket(pack, h.player)
}

func (h *legacyHandler) HandleResponseResult(queued *Info, bundle *ResponseBundle) (handled bool, err error) {
	return handleResponseResult(queued, bundle, h.player)
}

func (h *legacyHandler) CheckAlreadyAppliedPack(hash []byte) error {
	return checkAlreadyAppliedPack(hash, h)
}
