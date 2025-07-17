package internal

import (
	"errors"
	"fmt"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/gjson"
	"log"
	"os"
	"strings"
	"testing"
)

type StoreTestSuit struct {
	suite.Suite
}

func (st *StoreTestSuit) TearDownSuite() {
	log.Println("reset database")
	if err := os.RemoveAll(DataDir()); err != nil {
		log.Fatal(err)
	}
}

func TestSchemaSpace(t *testing.T) {
	assert.Equal(t, "_schema:*", SchemaSpace())
}

func TestStoreTestSuit(t *testing.T) {
	suite.Run(t, new(StoreTestSuit))
}

func (st *StoreTestSuit) TestRegistry() {
	var err error
	schema := &Schema{
		Name:      lo.RandomString(10, lo.LowerCaseLettersCharset),
		Namespace: lo.RandomString(5, lo.LowerCaseLettersCharset),
	}
	err = StoreImpl().Registry(schema)
	assert.Nil(st.T(), err)
	var value string
	_ = StoreImpl().impl.View(func(tx *buntdb.Tx) error {
		value, err = tx.Get(schema.Key())
		return err
	})
	assert.Nil(st.T(), err, "database should be registered successfully")
	assert.True(st.T(), len(value) > 0 && gjson.Get(value, "namespace").Str == schema.Namespace)
	v, ok := StoreImpl().schemas[schema.Name]
	assert.True(st.T(), ok)
	assert.Equal(st.T(), v.Name, schema.Name)

	// register again nano.ErrDbExists should be thrown out
	err = StoreImpl().Registry(schema)
	assert.True(st.T(), errors.Is(err, ErrDbExists))
	assert.True(st.T(), strings.Contains(err.Error(), schema.Name))
	assert.True(st.T(), strings.HasPrefix(err.Error(), ErrDbExists.Error()))

	// test different name but with same prefix
	schema1 := schema.Clone()
	schema1.Name = fmt.Sprintf("123%s", schema.Name)
	err = StoreImpl().Registry(schema1)
	assert.True(st.T(), errors.Is(err, ErrNamespaceExists))
	assert.True(st.T(), strings.HasPrefix(err.Error(), ErrNamespaceExists.Error()))

	// register another database
	schema2 := &Schema{
		Name:      fmt.Sprintf("%s%s", lo.RandomString(3, lo.LowerCaseLettersCharset), schema.Name),
		Namespace: lo.RandomString(5, lo.LowerCaseLettersCharset),
	}
	err = StoreImpl().Registry(schema2)
	assert.Nil(st.T(), err, "should not throw error %s", schema2.Name)
	//
	//var dbs []string
	//_ = StoreImpl().impl.View(func(tx *buntdb.Tx) error {
	//	return tx.AscendKeys(SchemaSpace(), func(key, value string) bool {
	//		if strings.HasSuffix(key, basic) {
	//			dbs = append(dbs, key)
	//		}
	//		return true
	//	})
	//})
	//assert.Len(st.T(), dbs, 2)
	//assert.True(st.T(), lo.EveryBy(dbs, func(item string) bool {
	//	return strings.HasSuffix(item, basic) && strings.HasPrefix(item, schemaSpacePrefix)
	//}), "should have suffix %s", basic)
	//lo.ForEach(lo.Map(dbs, func(item string, _ int) string {
	//	return strings.Split(item, keySeparator)[1]
	//}), func(item string, _ int) {
	//	_, ok = StoreImpl().schemas[item]
	//	assert.True(st.T(), ok, "db should also exists in cache")
	//})
}
