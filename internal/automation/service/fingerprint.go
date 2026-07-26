package service

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/output"
)

func actionFingerprint(def automation.Definition, result output.Result) string {
	payload := struct {
		Actions []automation.Action `json:"actions"`
		Result  any                 `json:"result"`
	}{Actions: def.Output.Actions, Result: result.JSON}
	if result.Mode == "text" {
		payload.Result = result.Text
	}
	raw, _ := json.Marshal(payload)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
