package arrowconv

import (
	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
	"github.com/apache/arrow/go/v7/arrow/memory"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
)

func TestArrowConverter_getSchema(t *testing.T) {
	record := make(map[string]interface{})
	record["colA"] = "1000"
	record["colB"] = float64(50000.00)
	record["colC"] = int32(800)

	conv := NewArrowConverter(memory.NewGoAllocator())
	schema, err := conv.getSchema(record)
	if err != nil {
		t.Fatal(err)
	}
	log.Println(schema.String())

	// verify
	colA, exists := schema.FieldsByName("colA")
	assert.True(t, exists, "colA should exist in the schema")
	assert.Equal(t, "colA", colA[0].Name, "the name of field colA must be correct")
	assert.Equal(t, arrow.STRING, colA[0].Type.ID(), "the type of column A must be a string")

	colB, exists := schema.FieldsByName("colB")
	assert.True(t, exists, "colB should exist in the schema")
	assert.Equal(t, "colB", colB[0].Name, "the name of field colB must be correct")
	assert.Equal(t, arrow.FLOAT64, colB[0].Type.ID(), "the type of column B must be a float64")

	colC, exists := schema.FieldsByName("colC")
	assert.True(t, exists, "colC should exist in the schema")
	assert.Equal(t, "colC", colC[0].Name, "the name of field colC must be correct")
	assert.Equal(t, arrow.INT32, colC[0].Type.ID(), "the type of column C must be an int32")

	//t.Fail()

}

func TestArrowConverter_getRecord(t *testing.T) {
	data := make(map[string]interface{})
	data["colA"] = "1000"
	data["colB"] = float64(50000.00)
	data["colC"] = int32(800)

	conv := NewArrowConverter(memory.NewGoAllocator())
	fields := []arrow.Field{
		{
			Name: "colA",
			Type: &arrow.StringType{},
			Nullable: false,
		},
		{
			Name: "colB",
			Type: &arrow.Float64Type{},
			Nullable: false,
		},
		{
			Name: "colC",
			Type: &arrow.Int32Type{},
			Nullable: false,
		},
	}
	schema := arrow.NewSchema(fields, nil)
	builder := array.NewRecordBuilder(conv.pool, schema)
	builder.Retain()

	// call the function
	result, err := conv.getRecord(data, builder)
	defer result.Release()

	assert.NoError(t, err, "there should be no error converting to an Arrow record")

	// assess the result
	log.Println(result)

}