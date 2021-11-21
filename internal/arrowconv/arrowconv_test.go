package arrowconv

import (
	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
	"github.com/apache/arrow/go/v7/arrow/memory"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
)

func getTestMap() map[string]interface{} {
	data := make(map[string]interface{})
	data["colA"] = "1000"
	data["colB"] = float64(50000.00)
	data["colC"] = int32(800)
	data["colD"] = true
	data["colE"] = []byte{10}
	data["colF"] = float32(1.0)
	data["colG"] = int64(10000)
	return data
}

func validateArrow(t *testing.T, result array.Record) {
	columnNumbers := make(map[string]int)
	for i, field := range result.Schema().Fields() {
		columnNumbers[field.Name] = i
	}
	assert.Equal(t, "1000", result.Column(columnNumbers["colA"]).(*array.String).Value(0),
		"columnA should have a value of '1000'")
	assert.Equal(t, float64(50000.00), result.Column(columnNumbers["colB"]).(*array.Float64).Value(0),
		"columnB should have a value of 50000.00")
	assert.Equal(t, int32(800), result.Column(columnNumbers["colC"]).(*array.Int32).Value(0),
		"columnC should have a value of 800")
	assert.Equal(t, true, result.Column(columnNumbers["colD"]).(*array.Boolean).Value(0),
		"columnD should have a value of true")
	assert.Equal(t, []byte{10}, result.Column(columnNumbers["colE"]).(*array.Binary).Value(0),
		"columnE should have a value of []byte{10}")
	assert.Equal(t, float32(1.0), result.Column(columnNumbers["colF"]).(*array.Float32).Value(0),
		"columnF should have a value of 1.0")
	assert.Equal(t, int64(10000), result.Column(columnNumbers["colG"]).(*array.Int64).Value(0),
		"columnG should have a value of 10000")
}

func validateMap(t *testing.T, result map[string]interface{}) {
	refData := getTestMap()
	for k, v := range result {
		assert.Equal(t, refData[k], v, "data should be equal to the data in the test map")
	}
}

func getTestFields() []arrow.Field {
	return []arrow.Field{
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
		{
			Name: "colD",
			Type: &arrow.BooleanType{},
			Nullable: false,
		},
		{
			Name: "colE",
			Type: &arrow.BinaryType{},
			Nullable: false,
		},
		{
			Name: "colF",
			Type: &arrow.Float32Type{},
			Nullable: false,
		},
		{
			Name: "colG",
			Type: &arrow.Int64Type{},
			Nullable: false,
		},
	}
}

func getArrowRecord(data map[string]interface{}, builder *array.RecordBuilder) array.Record {
	defer builder.Release()
	for i, field := range builder.Schema().Fields() {
		val := data[field.Name]
		switch field.Type.ID() {
		case arrow.BOOL:
			builder.Field(i).(*array.BooleanBuilder).Append(val.(bool))
		case arrow.BINARY:
			builder.Field(i).(*array.BinaryBuilder).Append(val.([]byte))
		case arrow.FLOAT32:
			builder.Field(i).(*array.Float32Builder).Append(val.(float32))
		case arrow.FLOAT64:
			builder.Field(i).(*array.Float64Builder).Append(val.(float64))
		case arrow.INT32:
			builder.Field(i).(*array.Int32Builder).Append(val.(int32))
		case arrow.INT64:
			builder.Field(i).(*array.Int64Builder).Append(val.(int64))
		case arrow.STRING:
			builder.Field(i).(*array.StringBuilder).Append(val.(string))
		}
	}
	return builder.NewRecord()
}

func TestArrowConverter_getSchema_not_handled_dtype(t *testing.T) {
	type AStruct struct{}

	row := make(map[string]interface{})
	row["colA"] = AStruct{}

	conv := NewArrowConverter(memory.NewGoAllocator())
	schema, err := conv.getSchema(row)
	assert.Nil(t, schema, "schema should be nil when an error occurred")
	assert.NotNil(t, err, "there should be an error when a type is passed in that does not have a conversion")
}

func TestArrowConverter_getSchema(t *testing.T) {
	record := getTestMap()

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

	colD, exists := schema.FieldsByName("colD")
	assert.True(t, exists, "colD should exist in the schema")
	assert.Equal(t, "colD", colD[0].Name, "the name of field colD must be correct")
	assert.Equal(t, arrow.BOOL, colD[0].Type.ID(), "the type of column D must be an bool")

	colE, exists := schema.FieldsByName("colE")
	assert.True(t, exists, "colE should exist in the schema")
	assert.Equal(t, "colE", colE[0].Name, "the name of field colE must be correct")
	assert.Equal(t, arrow.BINARY, colE[0].Type.ID(), "the type of column E must be binary")

	colF, exists := schema.FieldsByName("colF")
	assert.True(t, exists, "colF should exist in the schema")
	assert.Equal(t, "colF", colF[0].Name, "the name of field colF must be correct")
	assert.Equal(t, arrow.FLOAT32, colF[0].Type.ID(), "the type of column F must be an float32")

	colG, exists := schema.FieldsByName("colG")
	assert.True(t, exists, "colC should exist in the schema")
	assert.Equal(t, "colG", colG[0].Name, "the name of field colG must be correct")
	assert.Equal(t, arrow.INT64, colG[0].Type.ID(), "the type of column G must be an int64")

}

func TestArrowConverter_getRecord(t *testing.T) {
	data := getTestMap()
	conv := NewArrowConverter(memory.NewGoAllocator())
	fields := getTestFields()
	schema := arrow.NewSchema(fields, nil)
	builder := array.NewRecordBuilder(conv.pool, schema)
	builder.Retain()

	// call the function
	result, err := conv.getRecord(data, builder)
	defer result.Release()
	assert.NoError(t, err, "there should be no error converting to an Arrow record")
	validateArrow(t, result)

}

func TestArrowConverter_getRecord_error(t *testing.T) {
	data := getTestMap()
	conv := NewArrowConverter(memory.NewGoAllocator())
	fields := getTestFields()
	fields[0].Type = &arrow.Date32Type{}
	fields[0].Type.ID()
	schema := arrow.NewSchema(fields, nil)
	builder := array.NewRecordBuilder(conv.pool, schema)
	builder.Retain()

	// call the function
	result, err := conv.getRecord(data, builder)
	if err == nil {
		defer result.Release()
	}

	assert.NotNil(t, err, "there should be an error converting a type to Arrow that has no valid conversion")
}

func TestArrowConverter_MapToArrow(t *testing.T) {
	data := getTestMap()
	conv := NewArrowConverter(memory.NewGoAllocator())
	result, err := conv.MapToArrow(data)
	if err == nil {
		defer result.Release()
	}
	assert.Nil(t, err, "there should be no error converting to arrow format from map")
	validateArrow(t, result)
}

func TestArrowConverter_MapToArrow_err(t *testing.T) {
	type FakeStruct struct{}
	data := getTestMap()
	data["colA"] = FakeStruct{}
	conv := NewArrowConverter(memory.NewGoAllocator())
	result, err := conv.MapToArrow(data)
	if err == nil {
		defer result.Release()
	}
	assert.NotNil(t, err, "there should be an error converting to arrow format with a type that has no valid conversion")
}


func TestArrowConverter_ArrowToMap(t *testing.T) {
	// set up.
	data := getTestMap()
	fields := getTestFields()
	schema := arrow.NewSchema(fields, nil)
	alloc := memory.NewGoAllocator()
	conv := NewArrowConverter(alloc)
	builder := array.NewRecordBuilder(alloc, schema)
	builder.Retain()
	record := getArrowRecord(data, builder)
	defer record.Release()

	// run the test.
	result, err := conv.ArrowToMap(record)
	assert.Nil(t, err, "there should be no error converting from arrow record to map")
	validateMap(t, result)
}
