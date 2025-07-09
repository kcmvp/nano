package internal

import (
	"errors"
	"fmt"
	"github.com/kcmvp/nano"
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
	name := lo.RandomString(10, lo.LowerCaseLettersCharset)
	basic := name
	prefix := lo.RandomString(5, lo.LowerCaseLettersCharset)
	err = IS().Registry(name, string(prefix))
	assert.Nil(st.T(), err)
	var value string
	_ = IS().impl.View(func(tx *buntdb.Tx) error {
		value, err = tx.Get(SchemaKey(name))
		return err
	})
	assert.Nil(st.T(), err, "database should be registered successfully")
	assert.True(st.T(), len(value) > 0 && gjson.Get(value, "namespace").Str == string(prefix))
	v, ok := IS().schemas.Load(name)
	assert.True(st.T(), ok)
	assert.Equal(st.T(), v.(*Schema).Name, name)

	// register again nano.ErrDbExists should be thrown out
	err = IS().Registry(name, string(prefix))
	assert.True(st.T(), errors.Is(err, nano.ErrDbExists))
	assert.True(st.T(), strings.Contains(err.Error(), name))
	assert.True(st.T(), strings.HasPrefix(err.Error(), nano.ErrDbExists.Error()))

	// test different name but with same prefix
	name = fmt.Sprintf("123%s", name)
	err = IS().Registry(name, string(prefix))
	assert.True(st.T(), errors.Is(err, nano.ErrSchemaExists))
	assert.True(st.T(), strings.HasPrefix(err.Error(), nano.ErrSchemaExists.Error()))

	// register another database
	prefix = lo.RandomString(5, lo.LowerCaseLettersCharset)
	err = IS().Registry(name, string(prefix))
	assert.Nil(st.T(), err, "should not throw error %s", name)

	var dbs []string
	_ = IS().impl.View(func(tx *buntdb.Tx) error {
		return tx.AscendKeys(SchemaSpace(), func(key, value string) bool {
			if strings.HasSuffix(key, basic) {
				dbs = append(dbs, key)
			}
			return true
		})
	})
	assert.Len(st.T(), dbs, 2)
	assert.True(st.T(), lo.EveryBy(dbs, func(item string) bool {
		return strings.HasSuffix(item, basic) && strings.HasPrefix(item, schemaSpacePrefix)
	}), "should have suffix %s", basic)
	lo.ForEach(lo.Map(dbs, func(item string, _ int) string {
		return strings.Split(item, keySeparator)[1]
	}), func(item string, _ int) {
		_, ok = IS().schemas.Load(item)
		assert.True(st.T(), ok, "db should also exists in cache")
	})
}
