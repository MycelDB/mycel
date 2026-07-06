package cmd

import "testing"

func TestSemanticProvisioningCLI(t *testing.T) {
	t.Skip("embedded semantic provisioning CLI removed in daemon-only phase 2; daemon provisioning coverage lives in admin inference/semantic tests")
}

func TestSemanticMigrateLegacyEmbeddingsCLI(t *testing.T) {
	t.Skip("embedded legacy embedding migration setup removed in daemon-only phase 2; daemon migration coverage lives in semantic migration tests")
}
