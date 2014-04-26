package gosandbox_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/superduper/gosandbox"
)

var _ = Describe("Importing packages", func() {
	Describe("Create struct from package", func() {
		sample := &gosandbox.SampleStruct{
			Name: "John",
			Id:   1,
		}

		It("should have name", func() {
			Expect(sample.Name).To(Equal("John"))
		})

	})
})
