package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	blobservice "github.com/myceldb/mycel/internal/blob/service"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graph "github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/inference/connectors"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestImageAnalysisProcedureLoadsImageBlobPayload(t *testing.T) {
	ctx := context.Background()
	inference, ids, fake := newAutomationInferenceRuntime(t, ctx, false)
	imageProfileID, _ := addAutomationImageAnalysisProfileGrant(t, ctx, inference, ids, "image-summary-with-payload", "principal-a")
	store := storage.NewFileStore(t.TempDir())
	blobID := uuid.NewString()
	imageBytes := []byte{0x89, 0x50, 0x4e, 0x47}
	nodeID := uuid.New()
	node := graph.Node{ID: graph.NodeID(nodeID), DomainID: ids.domainID, Labels: []string{"Image"}, Payload: map[string]any{"blob_id": blobID, "mime_type": "image/png"}, Properties: map[string]any{"caption": "diagram"}}
	graphs := &automationE2EGraph{node: node}
	sessions := automationE2ESessions{spaceID: ids.spaceID, domainID: ids.domainID.String()}
	blobs := fakeAutomationBlobManager{blobs: map[string]fakeAutomationBlob{blobID: {meta: blobservice.BlobMeta{BlobID: blobID, SpaceID: ids.spaceID, DomainID: ids.domainID.String(), MimeType: "image/png", SizeBytes: int64(len(imageBytes)), CreateTime: time.Now().UTC()}, data: imageBytes}}}
	mgr := NewManager(store).WithGraphRuntime(sessions, graphs).WithInferenceManager(inference).WithBlobManager(blobs)
	procedure := automation.Procedure{ID: "image-analysis-payload", Version: 1, DomainID: ids.domainID, Status: automation.StatusEnabled, Input: automation.Input{Target: "changed", Fields: []string{"properties.caption"}}, Inference: automation.InferenceRef{Operation: string(domaininference.OperationImageAnalysis), ProfileID: imageProfileID.String()}, Prompt: "Describe the image", Output: automation.Output{Mode: automation.OutputModeText, Actions: []automation.Action{{UpdateNode: &automation.UpdateNodeAction{Target: "changed", Set: map[string]string{"properties.image_summary": "$result.text"}}}}}}
	binding := automation.Binding{ID: "image-analysis-payload-binding", Version: 1, DomainID: ids.domainID, ProcedureID: procedure.ID, ProcedureVersion: procedure.Version, Status: automation.StatusEnabled, Scope: automation.BindingScope{SpaceID: ids.spaceID, DomainID: ids.domainID}, Trigger: automation.BindingTrigger{Type: automation.TriggerTypeGraphEvent, Events: []string{automation.EventNodeCreated}, Labels: []string{"Image"}}, Runtime: automation.RuntimeContext{ActorPrincipalID: automationActor, OwnerPrincipalID: "principal-a", OnBehalfOfPrincipalID: "principal-a", InferenceProfileID: imageProfileID.String()}}
	putProcedureAndBinding(t, ctx, store, procedure, binding)
	emitNodeCreated(t, ctx, mgr, ids.spaceID, ids.domainID, "operator-admin", node)
	if processed, err := mgr.ProcessPending(ctx, ids.domainID, 10); err != nil || processed != 1 {
		t.Fatalf("ProcessPending() processed=%d err=%v", processed, err)
	}
	last := fake.LastChat()
	images := imageParts(last.Messages)
	if len(images) != 1 || images[0].BlobID != blobID || images[0].MimeType != "image/png" || !bytes.Equal(images[0].Data, imageBytes) {
		t.Fatalf("connector image inputs = %+v", images)
	}
	if got := graphs.node.Properties["image_summary"]; got != "result text" {
		t.Fatalf("image summary property = %#v", got)
	}
	events, err := inference.UsageLedger().ListUsageEvents(ctx)
	if err != nil || len(events) != 1 {
		t.Fatalf("usage events = %+v err=%v", events, err)
	}
	if events[0].Metadata["image_input_count"] != 1 || events[0].Metadata["image_input_total_bytes"] != float64(len(imageBytes)) && events[0].Metadata["image_input_total_bytes"] != int64(len(imageBytes)) {
		t.Fatalf("usage metadata should contain payload-neutral image diagnostics: %+v", events[0].Metadata)
	}
	rawEvents, _ := json.Marshal(events)
	if strings.Contains(string(rawEvents), "iVBORw") || strings.Contains(string(rawEvents), string(imageBytes)) {
		t.Fatalf("usage events leaked image payload: %s", rawEvents)
	}
}

func TestImageAnalysisProcedureFailsClosedForInvalidImageBlobs(t *testing.T) {
	cases := []struct {
		name    string
		blob    *fakeAutomationBlob
		wantErr string
	}{
		{name: "missing", wantErr: "not found"},
		{name: "unsupported_mime", blob: &fakeAutomationBlob{meta: blobservice.BlobMeta{MimeType: "application/pdf", SizeBytes: 3}, data: []byte("pdf")}, wantErr: "unsupported MIME type"},
		{name: "oversized", blob: &fakeAutomationBlob{meta: blobservice.BlobMeta{MimeType: "image/png", SizeBytes: maxImageAnalysisImageBytes + 1}, data: []byte{0x89, 0x50}}, wantErr: "exceeds per-image size limit"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			inference, ids, fake := newAutomationInferenceRuntime(t, ctx, false)
			imageProfileID, _ := addAutomationImageAnalysisProfileGrant(t, ctx, inference, ids, "image-summary-invalid-"+tc.name, "principal-a")
			store := storage.NewFileStore(t.TempDir())
			blobID := uuid.NewString()
			nodeID := uuid.New()
			node := graph.Node{ID: graph.NodeID(nodeID), DomainID: ids.domainID, Labels: []string{"Image"}, Payload: map[string]any{"blob_id": blobID}}
			graphs := &automationE2EGraph{node: node}
			sessions := automationE2ESessions{spaceID: ids.spaceID, domainID: ids.domainID.String()}
			blobMap := map[string]fakeAutomationBlob{}
			if tc.blob != nil {
				blob := *tc.blob
				blob.meta.BlobID = blobID
				blob.meta.SpaceID = ids.spaceID
				blob.meta.DomainID = ids.domainID.String()
				blobMap[blobID] = blob
			}
			mgr := NewManager(store).WithGraphRuntime(sessions, graphs).WithInferenceManager(inference).WithBlobManager(fakeAutomationBlobManager{blobs: blobMap})
			procedure := automation.Procedure{ID: "image-analysis-invalid-" + tc.name, Version: 1, DomainID: ids.domainID, Status: automation.StatusEnabled, Input: automation.Input{Target: "changed", Fields: []string{"payload.blob_id"}}, Inference: automation.InferenceRef{Operation: string(domaininference.OperationImageAnalysis), ProfileID: imageProfileID.String()}, Prompt: "Describe the image", Output: automation.Output{Mode: automation.OutputModeText, Actions: []automation.Action{{UpdateNode: &automation.UpdateNodeAction{Target: "changed", Set: map[string]string{"properties.image_summary": "$result.text"}}}}}}
			binding := automation.Binding{ID: "image-analysis-invalid-binding-" + tc.name, Version: 1, DomainID: ids.domainID, ProcedureID: procedure.ID, ProcedureVersion: procedure.Version, Status: automation.StatusEnabled, Scope: automation.BindingScope{SpaceID: ids.spaceID, DomainID: ids.domainID}, Trigger: automation.BindingTrigger{Type: automation.TriggerTypeGraphEvent, Events: []string{automation.EventNodeCreated}, Labels: []string{"Image"}}, Runtime: automation.RuntimeContext{ActorPrincipalID: automationActor, OwnerPrincipalID: "principal-a", OnBehalfOfPrincipalID: "principal-a", InferenceProfileID: imageProfileID.String()}}
			putProcedureAndBinding(t, ctx, store, procedure, binding)
			emitNodeCreated(t, ctx, mgr, ids.spaceID, ids.domainID, "operator-admin", node)
			if processed, err := mgr.ProcessPending(ctx, ids.domainID, 10); err != nil || processed != 1 {
				t.Fatalf("ProcessPending() processed=%d err=%v", processed, err)
			}
			_, chatCalls := fake.Calls()
			if chatCalls != 0 {
				t.Fatalf("invalid image blob should fail before connector call, got %d calls", chatCalls)
			}
			invs, err := store.ListInvocations(ctx, ids.domainID, storage.InvocationFilter{})
			if err != nil || len(invs) != 1 || invs[0].Status != "failed" || !strings.Contains(invs[0].SkipReason, tc.wantErr) {
				t.Fatalf("invocation = %+v err=%v", invs, err)
			}
			run, err := store.GetRun(ctx, ids.domainID, invs[0].ID)
			if err != nil || run.Status != "failed" || !strings.Contains(run.Error, tc.wantErr) {
				t.Fatalf("run = %+v err=%v", run, err)
			}
		})
	}
}

func emitNodeCreated(t *testing.T, ctx context.Context, mgr *AutomationManager, spaceID string, domainID graph.DomainID, origin string, node graph.Node) {
	t.Helper()
	event := graphchange.CommittedEvent{ID: uuid.New(), SpaceID: domainspace.SpaceID(uuid.MustParse(spaceID)), DomainID: domainID, Origin: graphchange.OriginMetadata{PrincipalID: origin}, Changes: []graphchange.Change{{Type: graphchange.ChangeTypeNodeCreated, NodeID: node.ID.String(), Node: &node}}}
	if err := mgr.HandleGraphChange(ctx, event); err != nil {
		t.Fatalf("HandleGraphChange() error = %v", err)
	}
}

func imageParts(messages []connectors.Message) []connectors.ImageInput {
	var images []connectors.ImageInput
	for _, msg := range messages {
		for _, part := range msg.Parts {
			if part.Image != nil {
				images = append(images, *part.Image)
			}
		}
	}
	return images
}

type fakeAutomationBlob struct {
	meta blobservice.BlobMeta
	data []byte
}

type fakeAutomationBlobManager struct {
	blobs map[string]fakeAutomationBlob
}

func (f fakeAutomationBlobManager) UploadBlob(context.Context, blobservice.UploadInput) (blobservice.BlobMeta, error) {
	return blobservice.BlobMeta{}, fmt.Errorf("unused")
}

func (f fakeAutomationBlobManager) GetBlob(ctx context.Context, spaceID string, blobID string) (blobservice.BlobMeta, error) {
	meta, _, err := f.OpenBlob(ctx, spaceID, blobID)
	return meta, err
}

func (f fakeAutomationBlobManager) GetBlobInDomain(ctx context.Context, spaceID string, domainID string, blobID string) (blobservice.BlobMeta, error) {
	meta, _, err := f.OpenBlobInDomain(ctx, spaceID, domainID, blobID)
	return meta, err
}

func (f fakeAutomationBlobManager) OpenBlob(ctx context.Context, spaceID string, blobID string) (blobservice.BlobMeta, io.ReadCloser, error) {
	return f.OpenBlobInDomain(ctx, spaceID, "", blobID)
}

func (f fakeAutomationBlobManager) OpenBlobInDomain(ctx context.Context, spaceID string, domainID string, blobID string) (blobservice.BlobMeta, io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return blobservice.BlobMeta{}, nil, err
	}
	blob, ok := f.blobs[blobID]
	if !ok {
		return blobservice.BlobMeta{}, nil, blobservice.ErrNotFound
	}
	meta := blob.meta
	if meta.BlobID == "" {
		meta.BlobID = blobID
	}
	if meta.SpaceID == "" {
		meta.SpaceID = spaceID
	}
	if meta.DomainID == "" {
		meta.DomainID = domainID
	}
	if meta.SizeBytes == 0 {
		meta.SizeBytes = int64(len(blob.data))
	}
	return meta, io.NopCloser(bytes.NewReader(blob.data)), nil
}

func (f fakeAutomationBlobManager) DeleteBlob(context.Context, string, string) (string, error) {
	return "", errors.New("unused")
}

func (f fakeAutomationBlobManager) DeleteBlobInDomain(context.Context, string, string, string) (string, error) {
	return "", errors.New("unused")
}
