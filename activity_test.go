package cockroachdb

import (
	"fmt"
	"os/exec"
	"testing"
	"time"

	"github.com/project-flogo/core/activity"
	"github.com/project-flogo/core/support/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type CockroachdbActivityTestSuite struct {
	suite.Suite
}

func TestCockroachdbActivityTestSuite(t *testing.T) {
	suite.Run(t, new(CockroachdbActivityTestSuite))
}

func (suite *CockroachdbActivityTestSuite) SetupSuite() {
	command := exec.Command("docker", "start", "postgres")
	err := command.Run()
	if err != nil {
		fmt.Println(err.Error())
		command := exec.Command("docker", "run", "-p", "5432:5432", "-e", "POSTGRES_USER=test", "-e", "POSTGRES_PASSWORD=test", "-e", "POSTGRES_DB=testdb", "--name", "postgres", "-d", "postgres")
		err := command.Run()
		if err != nil {
			fmt.Println(err.Error())
			panic(err)
		}
		time.Sleep(10 * time.Second)
	}
}

func (suite *CockroachdbActivityTestSuite) SetupTest() {}

func (suite *CockroachdbActivityTestSuite) TestCockroachdbActivity_Register() {

	ref := activity.GetRef(&Activity{})
	act := activity.Get(ref)

	assert.NotNil(suite.T(), act)
}

func (suite *CockroachdbActivityTestSuite) TestCockroachdbActivity_Settings() {
	t := suite.T()

	settings := &Settings{
		Host: "localhost:5432",
		Database: "testdb",
		User: "test",
		Password: "test",
	}

	iCtx := test.NewActivityInitContext(settings, nil)
	_, err := New(iCtx)
	assert.Nil(t, err)
}

func (suite *CockroachdbActivityTestSuite) TestCockroachdbActivity_Insert() {
	t := suite.T()
	
	settings := &Settings{
		Host: "localhost:5432",
		Database: "testdb",
		User: "test",
		Password: "test",
		DataMapping: map[string]interface{}{
			"testtable": map[string]interface{}{
				"method": "INSERT",
				"data": map[string]interface{}{
					"id": "$.id",
					"dummy": "$.dummy",
				},
			},
		},
	}

	iCtx := test.NewActivityInitContext(settings, nil)
	act, err := New(iCtx)
	assert.Nil(t, err)

	tc := test.NewActivityContext(act.Metadata())
	tc.SetInput("data", []byte("{\"dummy\": \"xyz\"}"))
	_, err = act.Eval(tc) 
	assert.Nil(t, err)

	output := tc.GetOutput("result").(map[string]interface{})
	assert.NotNil(t, output["lastInsertedId"])
	assert.Equal(t, int64(1), output["rowsAffected"])
}

func (suite *CockroachdbActivityTestSuite) TestCockroachdbActivity_Update() {
	t := suite.T()
	
	settings := &Settings{
		Host: "localhost:5432",
		Database: "testdb",
		User: "test",
		Password: "test",
		DataMapping: map[string]interface{}{
			"testtable": map[string]interface{}{
				"method": "INSERT",
				"data": map[string]interface{}{
					"id": "$.id",
					"dummy": "$.dummy",
				},
			},
		},
	}

	iCtx := test.NewActivityInitContext(settings, nil)
	act, err := New(iCtx)
	assert.Nil(t, err)

	tc := test.NewActivityContext(act.Metadata())
	tc.SetInput("data", []byte("{\"dummy\": \"xyz\"}"))
	_, err = act.Eval(tc) 
	assert.Nil(t, err)

	output := tc.GetOutput("result").(map[string]interface{})
	assert.NotNil(t, output["lastInsertedId"])
	assert.Equal(t, int64(1), output["rowsAffected"])
	lastInsertedId := output["lastInsertedId"]

	settings = &Settings{
		Host: "localhost:5432",
		Database: "testdb",
		User: "test",
		Password: "test",
		DataMapping: map[string]interface{}{
			"testtable": map[string]interface{}{
				"method": "UPDATE",
				"data": map[string]interface{}{
					"id": "$.id",
					"dummy": "$.dummy",
				},
			},
		},
	}

	iCtx = test.NewActivityInitContext(settings, nil)
	act, err = New(iCtx)
	assert.Nil(t, err)

	tc = test.NewActivityContext(act.Metadata())
	dataString := fmt.Sprintf("{\"id\": \"%v\", \"dummy\": \"abc\"}", lastInsertedId)
	tc.SetInput("data", []byte(dataString))
	_, err = act.Eval(tc) 
	assert.Nil(t, err)

	output = tc.GetOutput("result").(map[string]interface{})
	assert.Equal(t, lastInsertedId, output["lastUpdatedId"])
	assert.Equal(t, int64(1), output["rowsAffected"])

}

func (suite *CockroachdbActivityTestSuite) TestCockroachdbActivity_Delete() {
	t := suite.T()
	
	settings := &Settings{
		Host: "localhost:5432",
		Database: "testdb",
		User: "test",
		Password: "test",
		DataMapping: map[string]interface{}{
			"testtable": map[string]interface{}{
				"method": "INSERT",
				"data": map[string]interface{}{
					"id": "$.id",
					"dummy": "$.dummy",
				},
			},
		},
	}

	iCtx := test.NewActivityInitContext(settings, nil)
	act, err := New(iCtx)
	assert.Nil(t, err)

	tc := test.NewActivityContext(act.Metadata())
	tc.SetInput("data", []byte("{\"dummy\": \"xyz\"}"))
	_, err = act.Eval(tc) 
	assert.Nil(t, err)

	output := tc.GetOutput("result").(map[string]interface{})
	assert.NotNil(t, output["lastInsertedId"])
	assert.Equal(t, int64(1), output["rowsAffected"])
	lastInsertedId := output["lastInsertedId"]

	settings = &Settings{
		Host: "localhost:5432",
		Database: "testdb",
		User: "test",
		Password: "test",
		DataMapping: map[string]interface{}{
			"testtable": map[string]interface{}{
				"method": "DELETE",
				"data": map[string]interface{}{
					"id": "$.id",
					"dummy": "$.dummy",
				},
			},
		},
	}

	iCtx = test.NewActivityInitContext(settings, nil)
	act, err = New(iCtx)
	assert.Nil(t, err)

	tc = test.NewActivityContext(act.Metadata())
	dataString := fmt.Sprintf("{\"id\": \"%v\", \"dummy\": \"abc\"}", lastInsertedId)
	tc.SetInput("data", []byte(dataString))
	_, err = act.Eval(tc) 
	assert.Nil(t, err)

	output = tc.GetOutput("result").(map[string]interface{})
	assert.Equal(t, lastInsertedId, output["lastDeletedId"])
	assert.Equal(t, int64(1), output["rowsDeleted"])

}