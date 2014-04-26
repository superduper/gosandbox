package main

import (
	"code.google.com/p/gcfg"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/dynamodb"
	"github.com/superduper/glog"
	"github.com/superduper/goannoying"
	"github.com/superduper/gocontract"
	"time"
)

const (
	DynamoDbDemoTable            = "DemoTable"
	TableStatusActive            = "ACTIVE"
	TableStatusCreating          = "CREATING"
	PrimaryKeyName               = "aggregateId"
	TableCreateCheckTimeout      = "20s"
	TableCreateCheckPollInterval = "3s"
)

func init() {
	glog.OverrideVerbosityFlag("9")
	glog.OverrideSeverityThreshold("info")
}

type TDynamoDBStore struct {
	dynamoServer *dynamodb.Server
	table        *dynamodb.Table
}

func MakeDynamoDBStore(awsAccessKey, awsSecretKey string) *TDynamoDBStore {
	var (
		auth aws.Auth
		pk   dynamodb.PrimaryKey
	)
	contract.RequireNoErrors(
		func() (err error) {
			auth, err = aws.GetAuth(awsAccessKey, awsSecretKey, auth.Token(), auth.Expiration())
			return
		},
		func() (err error) {
			desc := DynamoDBDemoTableDescription()
			pk, err = desc.BuildPrimaryKey()
			return
		})

	dynamo := dynamodb.Server{auth, aws.USWest2} // hardcode ftw
	table := dynamo.NewTable(DynamoDbDemoTable, pk)
	return &TDynamoDBStore{&dynamo, table}
}

func DynamoDBDemoTableDescription() dynamodb.TableDescriptionT {
	return dynamodb.TableDescriptionT{
		TableName: DynamoDbDemoTable,
		AttributeDefinitions: []dynamodb.AttributeDefinitionT{
			dynamodb.AttributeDefinitionT{PrimaryKeyName, "S"},
		},
		KeySchema: []dynamodb.KeySchemaT{
			dynamodb.KeySchemaT{PrimaryKeyName, "HASH"},
		},
		ProvisionedThroughput: dynamodb.ProvisionedThroughputT{
			ReadCapacityUnits:  1,
			WriteCapacityUnits: 1,
		},
	}
}

func (self *TDynamoDBStore) findTableByName(name string) bool {
	glog.Infof("Searching for table %s in table list", name)
	tables, err := self.dynamoServer.ListTables()
	glog.Infof("Got table list: %v", tables)
	contract.RequireNoError(err)
	for _, t := range tables {
		if t == name {
			glog.Infof("Found table %s", name)
			return true
		}
	}
	glog.Infof("Table %s wasnt found", name)
	return false
}

func (self *TDynamoDBStore) waitUntilTableIsActive(table string) {
	checkTimeout, _ := time.ParseDuration(TableCreateCheckTimeout)
	checkInterval, _ := time.ParseDuration(TableCreateCheckPollInterval)
	ok, err := annoying.WaitUntil("table active", func() (status bool, err error) {
		status = false
		desc, err := self.dynamoServer.DescribeTable(table)
		if err != nil {
			return
		}
		if desc.TableStatus == TableStatusActive {
			status = true
			return
		}
		return
	}, checkInterval, checkTimeout)
	if !ok {
		glog.Fatalf("Failed with: %s", err.Error())
	}
}

func (self *TDynamoDBStore) InitTable() {
	glog.Info("Initializing tables")
	newTableDesc := DynamoDBDemoTableDescription()
	tableExists := self.findTableByName(newTableDesc.TableName)
	if tableExists {
		glog.Infof("Table %s exists, skipping init", newTableDesc.TableName)
		glog.Infof("Waiting until table %s becomes active", newTableDesc.TableName)
		self.waitUntilTableIsActive(newTableDesc.TableName)
		glog.Infof("Table %s is active", newTableDesc.TableName)
		return
	} else {
		glog.Infof("Creating table %s", newTableDesc.TableName)
		status, err := self.dynamoServer.CreateTable(newTableDesc)
		contract.RequireNoError(err)
		if status == TableStatusCreating {
			glog.Infof("Waiting until table %s becomes active", newTableDesc.TableName)
			self.waitUntilTableIsActive(newTableDesc.TableName)
			glog.Infof("Table %s is active", newTableDesc.TableName)
			return
		}
		if status == TableStatusActive {
			glog.Infof("Table %s is active", newTableDesc.TableName)
			return
		}
		glog.Fatal("Unexpected status:", status)
	}
}

func (self *TDynamoDBStore) DestroyTable() {
	glog.Info("Destroying tables")
	newTableDesc := DynamoDBDemoTableDescription()
	tableExists := self.findTableByName(newTableDesc.TableName)
	if !tableExists {
		glog.Infof("Table %s doesn't exists, skipping deletion", newTableDesc.TableName)
		return
	} else {
		_, err := self.dynamoServer.DeleteTable(newTableDesc)
		if err != nil {
			glog.Fatal(err)
		}
		glog.Infof("Table %s deleted successfully", newTableDesc.TableName)
	}
}

func (self *TDynamoDBStore) PutItem(item *Item) (bool, error) {
	glog.Infof("Inserting item: %v", item)
	ok, err := self.table.PutItem(item.PrimaryKey, item.RangeKey, item.Attrs)
	if ok {
		glog.Infof("Succeed insert item: %v", item)
	}
	return ok, err
}

func (self *TDynamoDBStore) DeleteItem(item *Item) (bool, error) {
	glog.Infof("Deleting item: %v", item)
	ok, err := self.table.DeleteItem(item.MakePrimaryKey())
	if ok {
		glog.Infof("Succeed delete item: %v", item)
	}
	return ok, err
}

func (self *TDynamoDBStore) UpdateItem(item *Item) (bool, error) {
	glog.Infof("Updating item: %v", item)
	ok, err := self.table.UpdateAttributes(item.MakePrimaryKey(), item.Attrs)
	if ok {
		glog.Infof("Succeed update item: %v", item)
	}
	return ok, err
}

func (self *TDynamoDBStore) GetItem(pk string) (*Item, error) {
	glog.Infof("Getting item with pk: %s", pk)
	attrMap, err := self.table.GetItem(&dynamodb.Key{HashKey: pk})
	if err == nil {
		glog.Infof("Succeed item %s fetch, got: %v", pk, attrMap)
	} else {
		return nil, err
	}
	attrSlice := []dynamodb.Attribute{}
	for _, v := range attrMap {
		attrSlice = append(attrSlice, *v)
	}
	item := MakeItem(pk, attrSlice...)
	return item, err
}

type Config struct {
	DynamoDemos_AWS struct {
		Key    string
		Secret string
	}
}

type Item struct {
	PrimaryKey string
	RangeKey   string
	Attrs      []dynamodb.Attribute
}

func (self *Item) MakePrimaryKey() *dynamodb.Key {
	return &dynamodb.Key{HashKey: self.PrimaryKey}
}

func MakeItem(pk string, attrs ...dynamodb.Attribute) *Item {
	// without range
	return &Item{pk, "", attrs}
}

func StringAttr(name, value string) dynamodb.Attribute {
	return *dynamodb.NewStringAttribute(name, value)
}

func main() {
	var cfg Config
	err := gcfg.ReadFileInto(&cfg, "dynamodb.gcfg.local")
	if err != nil {
		glog.Fatal(err)
	}
	store := MakeDynamoDBStore(cfg.DynamoDemos_AWS.Key, cfg.DynamoDemos_AWS.Secret)
	store.InitTable()
	item := MakeItem("some-unique-id", StringAttr("color", "red"))
	// insert row
	if ok, err := store.PutItem(item); !ok {
		glog.Fatalf("Failed to save item %v because of: %v", item, err)
	}
	glog.Infof("Item %v saved ", item)
	// find row
	savedItem, err := store.GetItem(item.PrimaryKey)
	if err != nil {
		glog.Fatalf("Failed to get saved item with pk %s", item.PrimaryKey)

	}
	glog.Infof("Got item %#v", savedItem)
	// find non-existing row
	_, err = store.GetItem("unknown")
	if err == nil {
		glog.Fatalf("Shouldnt get an item with pk %s", "unknown")

	} else if err.Error() == "Item not found" {
		glog.Infof("Failed to get non-existent item with err %s", err.Error())
	} else {
		glog.Fatalf("Failed to get non-existent item with unexpected err %s", err.Error())
	}
	// update row
	updItem := MakeItem("some-unique-id", StringAttr("color", "violet"))
	if ok, err := store.UpdateItem(updItem); !ok {
		glog.Fatalf("Failed to update item with unexpected err: %v", err)
	}
	// delete row
	if ok, err := store.DeleteItem(item); !ok {
		glog.Fatalf("Failed to delete item with unexpected err: %v", err)
	}
	store.DestroyTable()
}
