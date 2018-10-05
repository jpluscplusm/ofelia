package core

import (
	"fmt"
	cfclient "github.com/cloudfoundry-community/go-cfclient"
	cfenv "github.com/cloudfoundry-community/go-cfenv"
	"github.com/jpillora/backoff"
	"net/url"
	"time"
)

type CloudFoundryTask struct {
	BareJob
	CloudFoundryCredentialsService string `gcfg:"cf-credentials-service"`
	CloudFoundryAppName            string `gcfg:"cf-appname"`
}

func NewCloudFoundryTask() *CloudFoundryTask {
	return &CloudFoundryTask{}
}

func (j *CloudFoundryTask) Run(ctx *Context) error {
	if !cfenv.IsRunningOnCF() {
		return fmt.Errorf("a cftask can only be run from an application running *inside* Cloud Foundry")
	}
	myEnv, err := cfenv.Current()
	if err != nil {
		return err
	}

	credentialsService, err := myEnv.Services.WithName(j.CloudFoundryCredentialsService)
	if err != nil {
		return err
	}

	c := &cfclient.Config{
		ApiAddress: myEnv.CFAPI,
		Username:   credentialsService.Credentials["username"].(string),
		Password:   credentialsService.Credentials["password"].(string),
	}
	client, err := cfclient.NewClient(c)
	if err != nil {
		return err
	}

	v := url.Values{}
	v.Set("q", fmt.Sprintf("name:%s", j.CloudFoundryAppName))
	v.Add("q", fmt.Sprintf("space_guid:%s", myEnv.SpaceID))
	app, err := client.ListAppsByQuery(v)
	if err != nil {
		return err
	}
	switch x := len(app); {
	case x == 1: // all is good
	case x == 0: // app not found
		return fmt.Errorf("Cloud Foundry app '%s' not present in space '%s'", j.CloudFoundryAppName, myEnv.SpaceName)
	case x > 1: // something odd happened - maybe our ListAppsByQuery() *wasn't* space-scoped?
		return fmt.Errorf("More than one Cloud Foundry app '%s' present in space '%s'", j.CloudFoundryAppName, myEnv.SpaceName)
	}
	task, err := client.CreateTask(cfclient.TaskRequest{
		Name:        j.Name,
		Command:     j.Command,
		DropletGUID: app[0].Guid,
	})
	if err != nil {
		return err
	}

	delay := &backoff.Backoff{
		Min:    2 * time.Second,
		Max:    10 * time.Second,
		Factor: 1.1,
		Jitter: true,
	}

	for {
		time.Sleep(delay.Duration())
		task, err = client.GetTaskByGuid(task.GUID)
		if err != nil {
			return err
		}
		switch task.State {
		case "RUNNING": // loop round again after sleeping
		case "SUCCEEDED":
			return nil
		case "FAILED":
			return fmt.Errorf("task failed")
		default:
			return fmt.Errorf("task state unknown: %s", task.State)
		}
	}
	return fmt.Errorf("not really sure how we got here ...")
}
