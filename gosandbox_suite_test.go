package gosandbox_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestGosandbox(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Gosandbox Suite")
}
