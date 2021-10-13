package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"

	"github.com/hashicorp/go-retryablehttp"
)

type TokenResult struct {
	SmallId uint64 `json:"SmallId"`
	LargeId uint64 `json:"LargeId"`
	Token   uint64 `json:"Token"`
	Key     string `json:"Key"`
}

type TeamInput struct {
	Data []*CompareResult `json:"data"`
}

type TeamOutput struct {
	Result []*TokenResult `json:"result"`
}

type ConvertorHandler struct {
	url               string
	payLoadSize       int
	TokenResultsChan  chan *TokenResult
	CompareResultChan chan *CompareResult
}

func NewConvertorHandler(compareResultChan chan *CompareResult, tokenResultsChan chan *TokenResult) *ConvertorHandler {
	payLoadSize, _ := strconv.Atoi(os.Getenv("PAYLOAD_SIZE"))
	converterIP := os.Getenv("CONVERTER_IP")
	return &ConvertorHandler{
		payLoadSize:       payLoadSize,
		TokenResultsChan:  tokenResultsChan,
		CompareResultChan: compareResultChan,
		url:               fmt.Sprintf("http://%s:8080/api/generateToken", converterIP),
	}
}

func (c *ConvertorHandler) Start() func() {
	return c.PrepareTokenMsg
}

func (c *ConvertorHandler) PrepareTokenMsg() {
	{
		number := c.payLoadSize
		teamInput := &TeamInput{}
		for compareResult := range c.CompareResultChan {
			teamInput.Data = append(teamInput.Data, compareResult)
			if len(teamInput.Data) == number {
				go c.getTokenRetry404(*teamInput, c.TokenResultsChan)
				teamInput.Data = teamInput.Data[:0]
			}
		}
	}
}

func (c *ConvertorHandler) getTokenRetry404(input TeamInput, outputChan chan *TokenResult) {
	client := retryablehttp.NewClient()
	client.Logger = nil
	client.RetryMax = 3
	client.CheckRetry = func(ctx context.Context, resp *http.Response, err error) (bool, error) {
		ok, e := retryablehttp.DefaultRetryPolicy(ctx, resp, err)
		if !ok && resp.StatusCode == 404 {
			return true, nil
		}
		return ok, e
	}
	body, err := json.Marshal(input)
	if err != nil {
		fmt.Printf("fail to marshal input data because: %v", err)
	}
	resp, err := client.Post(c.url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		fmt.Printf("get token with err: %v", err)
	}
	defer resp.Body.Close()
	output := TeamOutput{}
	if err = json.NewDecoder(resp.Body).Decode(&output); err != nil {
		fmt.Printf("failed to decode response from converter server: %v", err)
	}
	for _, rt := range output.Result {
		r := rt
		outputChan <- r
	}
}
