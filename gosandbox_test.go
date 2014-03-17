package gosandbox_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/superduper/gosandbox"
)

var _ = Describe("Importing packages", func() {
	Describe("Create struct from package", func() {
		sample := &gosandbox.SampleStruct{
			name: "John",
			id:   1,
		}

		It("should have name", func() {
			Expect(sample.name).To(Equal("John"))
		})

	})
})
