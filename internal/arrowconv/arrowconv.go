package arrowconv

import (
	"errors"
	"fmt"
	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
	"github.com/apache/arrow/go/v7/arrow/memory"
)

type ArrowConverter struct {
	pool *memory.GoAllocator
}

func NewArrowConverter(pool *memory.GoAllocator) *ArrowConverter {
	return &ArrowConverter{pool}
}

func (c *ArrowConverter) getSchema(data map[string]interface{}) (*arrow.Schema, error) {
	fields := make([]arrow.Field, 0)
	for k, v := range data {
		switch v.(type){
		case bool:
			field := arrow.Field{
				Name: k,
				Type: &arrow.BooleanType{},
				Nullable: false,
			}
			fields = append(fields, field)
		case []byte:
			field := arrow.Field{
				Name: k,
				Type: &arrow.BinaryType{},
				Nullable: false,
			}
			fields = append(fields, field)
		case float32:
			field := arrow.Field{
				Name: k,
				Type: &arrow.Float32Type{},
				Nullable: false,
			}
			fields = append(fields, field)
		case float64:
			field := arrow.Field{
				Name: k,
				Type: &arrow.Float64Type{},
				Nullable: false,
			}
			fields = append(fields, field)
		case int32:
			field := arrow.Field{
				Name: k,
				Type: &arrow.Int32Type{},
				Nullable: false,
			}
			fields = append(fields, field)
		case int64:
			field := arrow.Field{
				Name: k,
				Type: &arrow.Int64Type{},
				Nullable: false,
			}
			fields = append(fields, field)
		case string:
			field := arrow.Field{
				Name: k,
				Type: &arrow.StringType{},
				Nullable: false,
			}
			fields = append(fields, field)
		default:
			return nil, errors.New("no valid conversion for type")
		}
	}
	meta := arrow.NewMetadata([]string{}, []string{})
	return arrow.NewSchema(fields, &meta), nil
}

func (c *ArrowConverter) getRecord(data map[string]interface{}, builder *array.RecordBuilder) (array.Record, error) {
	defer builder.Release()
	schema := builder.Schema()
	for i, field := range schema.Fields() {
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
		default:
			fmt.Println("got a type we can't handle")
			return nil, errors.New("got a type we can't handle")
		}
	}
	record := builder.NewRecord()
	record.Retain()
	return record, nil
}

func (c *ArrowConverter) MapToArrow(data map[string]interface{}) (array.Record, error) {
	schema, err := c.getSchema(data)
	if err != nil {
		return nil, err
	}
	builder := array.NewRecordBuilder(c.pool, schema)
	builder.Retain()

	return c.getRecord(data, builder)
}

func (c *ArrowConverter) ArrowToMap(record array.Record) (map[string]interface{}, error) {
	defer record.Release()
	result := make(map[string]interface{})
	s := record.Schema()
	for i, column := range record.Columns() {
		field := s.Field(i)
		switch field.Type.ID() {
		case arrow.BOOL:
			col, ok := column.(*array.Boolean)
			if !ok {
				return result, errors.New("could not convert column to *array.Boolean")
			}
			result[field.Name] = col.Value(0)
		case arrow.BINARY:
			col, ok := column.(*array.Binary)
			if !ok {
				return result, errors.New("could not convert column to *array.Binary")
			}
			result[field.Name] = col.Value(0)
		case arrow.FLOAT32:
			col, ok := column.(*array.Float32)
			if !ok {
				return result, errors.New("could not convert column to *array.Float32")
			}
			result[field.Name] = col.Value(0)
		case arrow.FLOAT64:
			col, ok := column.(*array.Float64)
			if !ok {
				return result, errors.New("could not convert column to *array.Float64")
			}
			result[field.Name] = col.Value(0)
		case arrow.INT32:
			col, ok := column.(*array.Int32)
			if !ok {
				return result, errors.New("could not convert column to *array.Int32")
			}
			result[field.Name] = col.Value(0)
		case arrow.INT64:
			col, ok := column.(*array.Int64)
			if !ok {
				return result, errors.New("could not convert column to *array.Int64")
			}
			result[field.Name] = col.Value(0)
		case arrow.STRING:
			col, ok := column.(*array.String)
			if !ok {
				return result, errors.New("could not convert column to *array.String")
			}
			result[field.Name] = col.Value(0)
		default:
			return nil, errors.New("no conversion from arrow type to avro")
		}
	}
	return result, nil
}