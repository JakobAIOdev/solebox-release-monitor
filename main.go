package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Config struct {
	SearchURL        string
	ProductsManyURL  string
	ProductIDs       []string
	DiscoveryURLs    []string
	ReleasePageURLs  []string
	DiscoveryPages   int
	DiscoveryPerPage int
	BatchSize        int
	PollInterval     time.Duration
	WebhookURL       string
	DiscordMention   string
	DiscordUsername  string
	BearerToken      string
	XCharybdis       string
	Referer          string
	UserAgent        string
	ResultBaseURL    string
	SeenFile         string
	DryRun           bool
	RunOnce          bool
	AlertStockDelta  int
	LockFile         string
}

type Result struct {
	Key             string
	Link            string
	Title           string
	Raw             any
	Snapshot        *ProductSnapshot
	Reason          string
	DiscoverySource string
}

type ProductSnapshot struct {
	ID          string      `json:"id,omitempty"`
	Key         string      `json:"key"`
	Title       string      `json:"title,omitempty"`
	Link        string      `json:"link,omitempty"`
	Image       string      `json:"image,omitempty"`
	Price       string      `json:"price,omitempty"`
	Retail      string      `json:"retail,omitempty"`
	SKU         string      `json:"sku,omitempty"`
	Brand       string      `json:"brand,omitempty"`
	Color       string      `json:"color,omitempty"`
	Status      string      `json:"status,omitempty"`
	InStock     bool        `json:"inStock"`
	TotalStock  int         `json:"totalStock"`
	Sizes       []SizeStock `json:"sizes,omitempty"`
	Fingerprint string      `json:"fingerprint"`
	SeenAt      string      `json:"seenAt"`
}

type SizeStock struct {
	Label   string `json:"label"`
	Stock   int    `json:"stock"`
	InStock bool   `json:"inStock"`
}

type MonitorState struct {
	Seen        map[string]bool            `json:"seen,omitempty"`
	Products    map[string]ProductSnapshot `json:"products,omitempty"`
	Discoveries map[string]map[string]bool `json:"discoveries,omitempty"`
	Updated     string                     `json:"updated,omitempty"`
}

type DiscordMessage struct {
	Content  string         `json:"content,omitempty"`
	Username string         `json:"username,omitempty"`
	Embeds   []DiscordEmbed `json:"embeds,omitempty"`
}

type DiscordEmbed struct {
	Title       string              `json:"title,omitempty"`
	URL         string              `json:"url,omitempty"`
	Description string              `json:"description,omitempty"`
	Color       int                 `json:"color,omitempty"`
	Timestamp   string              `json:"timestamp,omitempty"`
	Fields      []DiscordEmbedField `json:"fields,omitempty"`
	Thumbnail   *DiscordEmbedImage  `json:"thumbnail,omitempty"`
	Image       *DiscordEmbedImage  `json:"image,omitempty"`
	Footer      *DiscordEmbedFooter `json:"footer,omitempty"`
}

type DiscordEmbedField struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Inline bool   `json:"inline"`
}

type DiscordEmbedImage struct {
	URL string `json:"url"`
}

type DiscordEmbedFooter struct {
	Text string `json:"text"`
}

type DiscordRateLimit struct {
	Message    string  `json:"message"`
	RetryAfter float64 `json:"retry_after"`
	Global     bool    `json:"global"`
}

type CharybdisPayload struct {
	Concept                string   `json:"concept"`
	ScayleEnvironment      string   `json:"scayleEnvironment"`
	ScayleShopID           string   `json:"scayleShopId"`
	CmsSpace               string   `json:"cmsSpace"`
	CmsEnvironment         string   `json:"cmsEnvironment"`
	CmsAccessToken         string   `json:"cmsAccessToken"`
	Tree                   string   `json:"tree"`
	Hades                  bool     `json:"hades"`
	CampaignKey            string   `json:"campaignKey"`
	Context                string   `json:"context"`
	UseMiwa                bool     `json:"useMiwa"`
	Loyalty                bool     `json:"loyalty"`
	LoyaltyCampaignKey     string   `json:"loyaltyCampaignKey"`
	Company                string   `json:"company"`
	NewsletterRegistration string   `json:"newsletterRegistration"`
	UserRegistration       string   `json:"userRegistration"`
	NotifyMeRegistration   int      `json:"notifyMeRegistration"`
	UpgradeUserService     int      `json:"upgradeUserService"`
	Has30DaysPriceFallback bool     `json:"has30DaysPriceFallback"`
	PhoneValidation        string   `json:"phoneValidation"`
	LoyaltyEngine          string   `json:"loyaltyEngine"`
	Source                 string   `json:"source"`
	Groups                 []string `json:"groups"`
	ZendeskSubdomain       string   `json:"zendeskSubdomain"`
	BasestoreURL           string   `json:"basestoreUrl"`
}

func main() {
	protectedEnv := currentEnvKeys()
	if err := loadDotEnv(".env", protectedEnv); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Fatalf("load .env: %v", err)
	}

	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}

	releaseLock, err := acquireLock(cfg.LockFile)
	if err != nil {
		log.Fatal(err)
	}
	defer releaseLock()

	client := &http.Client{Timeout: 12 * time.Second}
	state, err := loadState(cfg.SeenFile)
	if err != nil {
		log.Fatalf("load seen file: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("monitor started: interval=%s seen=%d products=%d", cfg.PollInterval, len(state.Seen), len(state.Products))
	if err := pollOnce(ctx, client, cfg, state); err != nil {
		log.Printf("initial poll failed: %v", err)
	}
	if cfg.RunOnce {
		log.Println("run once complete")
		return
	}

	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("monitor stopped")
			return
		case <-ticker.C:
			if err := loadDotEnv(".env", protectedEnv); err != nil && !errors.Is(err, os.ErrNotExist) {
				log.Printf("reload .env failed: %v", err)
			}
			if reloaded, err := loadConfig(); err != nil {
				log.Printf("reload config failed: %v", err)
			} else {
				cfg = reloaded
			}
			if err := pollOnce(ctx, client, cfg, state); err != nil {
				log.Printf("poll failed: %v", err)
			}
		}
	}
}

func acquireLock(path string) (func(), error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return func() {}, nil
	}
	if err := writeLockFile(path); err == nil {
		return func() {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				log.Printf("remove lock file failed: %v", err)
			}
		}, nil
	} else if !errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("create lock file: %w", err)
	}

	body, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read lock file %s: %w", path, err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(body)))
	if err == nil && pid > 0 && processExists(pid) {
		return nil, fmt.Errorf("monitor already running with pid %d (lock file %s)", pid, path)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("remove stale lock file %s: %w", path, err)
	}
	if err := writeLockFile(path); err != nil {
		return nil, fmt.Errorf("create lock file after stale cleanup: %w", err)
	}
	return func() {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Printf("remove lock file failed: %v", err)
		}
	}, nil
}

func writeLockFile(path string) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = fmt.Fprintf(file, "%d\n", os.Getpid())
	return err
}

func processExists(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func loadConfig() (Config, error) {
	interval := getenv("POLL_INTERVAL", "30s")
	pollInterval, err := time.ParseDuration(interval)
	if err != nil {
		return Config{}, fmt.Errorf("invalid POLL_INTERVAL %q: %w", interval, err)
	}
	if pollInterval < 5*time.Second {
		return Config{}, fmt.Errorf("POLL_INTERVAL must be at least 5s")
	}
	batchSize := getenvInt("PRODUCT_BATCH_SIZE", 50)
	if batchSize < 1 || batchSize > 100 {
		return Config{}, fmt.Errorf("PRODUCT_BATCH_SIZE must be between 1 and 100")
	}
	discoveryPages := getenvInt("DISCOVERY_PAGES", 1)
	if discoveryPages < 1 || discoveryPages > 10 {
		return Config{}, fmt.Errorf("DISCOVERY_PAGES must be between 1 and 10")
	}
	discoveryPerPage := getenvInt("DISCOVERY_PER_PAGE", 48)
	if discoveryPerPage < 1 || discoveryPerPage > 100 {
		return Config{}, fmt.Errorf("DISCOVERY_PER_PAGE must be between 1 and 100")
	}

	cfg := Config{
		SearchURL:        strings.TrimSpace(os.Getenv("SEARCH_URL")),
		ProductsManyURL:  strings.TrimSpace(os.Getenv("PRODUCTS_MANY_URL")),
		ProductIDs:       splitCSV(os.Getenv("PRODUCT_IDS")),
		DiscoveryURLs:    splitCSV(os.Getenv("DISCOVERY_CATEGORY_URLS")),
		ReleasePageURLs:  splitCSV(os.Getenv("RELEASE_PAGE_URLS")),
		DiscoveryPages:   discoveryPages,
		DiscoveryPerPage: discoveryPerPage,
		BatchSize:        batchSize,
		PollInterval:     pollInterval,
		WebhookURL:       strings.TrimSpace(os.Getenv("DISCORD_WEBHOOK_URL")),
		DiscordMention:   strings.TrimSpace(os.Getenv("DISCORD_MENTION")),
		DiscordUsername:  getenv("DISCORD_USERNAME", "Solebox Monitor"),
		BearerToken:      strings.TrimSpace(os.Getenv("AUTHORIZATION_BEARER")),
		XCharybdis:       strings.TrimSpace(os.Getenv("X_CHARYBDIS")),
		Referer:          strings.TrimSpace(os.Getenv("REFERER")),
		UserAgent:        getenv("USER_AGENT", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36"),
		ResultBaseURL:    strings.TrimRight(strings.TrimSpace(os.Getenv("RESULT_BASE_URL")), "/"),
		SeenFile:         getenv("SEEN_FILE", "seen.json"),
		DryRun:           getenvBool("DRY_RUN", false),
		RunOnce:          getenvBool("RUN_ONCE", false),
		AlertStockDelta:  getenvInt("ALERT_STOCK_DELTA", 0),
		LockFile:         getenv("LOCK_FILE", ".solebox-monitor.lock"),
	}

	var missing []string
	if cfg.SearchURL == "" && cfg.ProductsManyURL == "" && len(cfg.DiscoveryURLs) == 0 && len(cfg.ReleasePageURLs) == 0 {
		missing = append(missing, "SEARCH_URL, PRODUCTS_MANY_URL, DISCOVERY_CATEGORY_URLS, or RELEASE_PAGE_URLS")
	}
	if cfg.WebhookURL == "" {
		missing = append(missing, "DISCORD_WEBHOOK_URL")
	}
	if cfg.XCharybdis == "" {
		generated, err := generateDefaultXCharybdis()
		if err != nil {
			missing = append(missing, "X_CHARYBDIS")
		} else {
			cfg.XCharybdis = generated
		}
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("missing required env values: %s", strings.Join(missing, ", "))
	}
	if cfg.ProductsManyURL == "" && strings.Contains(cfg.SearchURL, "/products/many") {
		cfg.ProductsManyURL = cfg.SearchURL
	}

	return cfg, nil
}

func generateDefaultXCharybdis() (string, error) {
	payload := CharybdisPayload{
		Concept:                getenv("CHARYBDIS_CONCEPT", "SOLE"),
		ScayleEnvironment:      getenv("CHARYBDIS_SCAYLE_ENVIRONMENT", "snipes-live"),
		ScayleShopID:           getenv("CHARYBDIS_SCAYLE_SHOP_ID", "1065"),
		CmsSpace:               getenv("CHARYBDIS_CMS_SPACE", "ahetnfemtyw6"),
		CmsEnvironment:         getenv("CHARYBDIS_CMS_ENVIRONMENT", "master"),
		CmsAccessToken:         getenv("CHARYBDIS_CMS_ACCESS_TOKEN", "EGR37iVNMxbL81kxwdLYPnBB10M3njZOOTUZGACpzv8"),
		Tree:                   getenv("CHARYBDIS_TREE", "32"),
		Hades:                  false,
		CampaignKey:            "",
		Context:                getenv("CHARYBDIS_CONTEXT", "de-de"),
		UseMiwa:                false,
		Loyalty:                false,
		LoyaltyCampaignKey:     "",
		Company:                getenv("CHARYBDIS_COMPANY", "lde"),
		NewsletterRegistration: "3",
		UserRegistration:       "3",
		NotifyMeRegistration:   0,
		UpgradeUserService:     0,
		Has30DaysPriceFallback: false,
		PhoneValidation:        "",
		LoyaltyEngine:          "undefined",
		Source:                 "Web",
		Groups:                 []string{"new"},
		ZendeskSubdomain:       "snipessupport",
		BasestoreURL:           "",
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(body), nil
}

func pollOnce(ctx context.Context, client *http.Client, cfg Config, state *MonitorState) error {
	results, err := fetchResults(ctx, client, cfg)
	if err != nil {
		return err
	}
	if len(results) == 0 {
		log.Printf("poll ok: no results found")
		return nil
	}

	var fresh []Result
	hadState := len(state.Seen) > 0 || len(state.Products) > 0
	hadProductState := len(state.Products) > 0
	changed := false
	now := time.Now().UTC().Format(time.RFC3339)
	for _, result := range results {
		if result.Snapshot == nil {
			if shouldAlertGenericResult(state, result) {
				if result.Reason == "" {
					result.Reason = "New release card"
				}
				fresh = append(fresh, result)
				changed = true
			}
			continue
		}
		snapshot := *result.Snapshot
		snapshot.SeenAt = now
		previous, ok := state.Products[snapshot.Key]
		if !ok {
			if shouldAlertNewProduct(state, result, hadProductState, snapshot) {
				result.Reason = "New product"
				fresh = append(fresh, result)
			}
			state.Products[snapshot.Key] = snapshot
			changed = true
			continue
		}
		if previous.Fingerprint != snapshot.Fingerprint {
			result.Reason = stockChangeReason(previous, snapshot)
			if hadState && shouldAlertStockChange(previous, snapshot, cfg.AlertStockDelta) {
				fresh = append(fresh, result)
			}
			state.Products[snapshot.Key] = snapshot
			changed = true
			continue
		}
		if productMetadataFingerprint(previous) != productMetadataFingerprint(snapshot) {
			state.Products[snapshot.Key] = snapshot
			changed = true
		}
	}
	if markDiscoveryBaselines(state, results) {
		changed = true
	}

	if !hadState {
		for _, result := range results {
			state.Seen[result.Key] = true
			if result.Snapshot != nil {
				snapshot := *result.Snapshot
				snapshot.SeenAt = now
				state.Products[snapshot.Key] = snapshot
			}
		}
		markDiscoveryBaselines(state, results)
		if err := saveState(cfg.SeenFile, state); err != nil {
			return err
		}
		log.Printf("seeded initial state with %d results", len(results))
		return nil
	}

	if len(fresh) > 0 {
		if cfg.DryRun {
			log.Printf("dry run: would send %d discord alerts", len(fresh))
		} else if err := sendDiscordBatches(ctx, client, cfg, fresh); err != nil {
			return fmt.Errorf("send discord webhook: %w", err)
		}
	}

	for _, result := range fresh {
		state.Seen[result.Key] = true
		log.Printf("alert: %s %s", result.Reason, displayResult(result))
	}

	if changed || len(fresh) > 0 {
		if err := saveState(cfg.SeenFile, state); err != nil {
			return err
		}
	}

	log.Printf("poll ok: results=%d alerts=%d tracked_products=%d", len(results), len(fresh), len(state.Products))
	return nil
}

func fetchSearch(ctx context.Context, client *http.Client, cfg Config) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.SearchURL, nil)
	if err != nil {
		return nil, 0, err
	}

	if cfg.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+strings.TrimPrefix(cfg.BearerToken, "Bearer "))
	}
	req.Header.Set("X-Charybdis", cfg.XCharybdis)
	req.Header.Set("User-Agent", cfg.UserAgent)
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("X-RECO-CAMPAIGN", "pdp_recommendations")
	req.Header.Set("sec-ch-ua-platform", `"macOS"`)
	req.Header.Set("sec-ch-ua", `"Chromium";v="148", "Google Chrome";v="148", "Not/A)Brand";v="99"`)
	req.Header.Set("sec-ch-ua-mobile", "?0")
	if cfg.Referer != "" {
		req.Header.Set("Referer", cfg.Referer)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return body, resp.StatusCode, nil
}

func fetchURL(ctx context.Context, client *http.Client, cfg Config, rawURL string) ([]byte, int, error) {
	tmp := cfg
	tmp.SearchURL = rawURL
	return fetchSearch(ctx, client, tmp)
}

func fetchResults(ctx context.Context, client *http.Client, cfg Config) ([]Result, error) {
	var all []Result
	if len(cfg.DiscoveryURLs) > 0 {
		discovered, err := fetchDiscoveryResults(ctx, client, cfg)
		if err != nil {
			return nil, err
		}
		all = append(all, discovered...)
	}
	if len(cfg.ReleasePageURLs) > 0 {
		releases, err := fetchReleasePageResults(ctx, client, cfg)
		if err != nil {
			return nil, err
		}
		all = append(all, releases...)
	}

	if cfg.ProductsManyURL != "" {
		ids := cfg.ProductIDs
		if len(ids) == 0 {
			ids = idsFromURL(cfg.ProductsManyURL)
		}
		if len(ids) > 0 {
			results, err := fetchProductBatches(ctx, client, cfg, ids)
			if err != nil {
				return nil, err
			}
			all = append(all, results...)
		}
	}
	if len(all) > 0 {
		return dedupeResults(all), nil
	}

	body, status, err := fetchSearch(ctx, client, cfg)
	if err != nil {
		return nil, err
	}
	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("unexpected status %d: %s", status, truncate(string(body), 500))
	}
	results, err := extractResults(body, cfg.ResultBaseURL)
	if err != nil {
		return nil, err
	}
	if cfg.ProductsManyURL == "" {
		return results, nil
	}

	discovered := resultIDs(results)
	if len(discovered) == 0 {
		return results, nil
	}
	stockResults, err := fetchProductBatches(ctx, client, cfg, discovered)
	if err != nil {
		return nil, err
	}
	return mergeResults(results, stockResults), nil
}

func fetchDiscoveryResults(ctx context.Context, client *http.Client, cfg Config) ([]Result, error) {
	var out []Result
	for _, discoveryURL := range cfg.DiscoveryURLs {
		ids, err := fetchDiscoveryIDs(ctx, client, cfg, discoveryURL)
		if err != nil {
			return nil, err
		}
		if len(ids) == 0 {
			continue
		}
		productsCfg := cfg
		if productsCfg.ProductsManyURL == "" {
			productsCfg.ProductsManyURL = defaultProductsManyURL(discoveryURL)
		}
		results, err := fetchProductBatches(ctx, client, productsCfg, ids)
		if err != nil {
			return nil, err
		}
		source := discoverySourceKey(discoveryURL)
		for i := range results {
			results[i].DiscoverySource = source
		}
		out = append(out, results...)
	}
	return out, nil
}

func fetchDiscoveryIDs(ctx context.Context, client *http.Client, cfg Config, discoveryURL string) ([]string, error) {
	var ids []string
	for page := 1; page <= cfg.DiscoveryPages; page++ {
		url, err := discoveryPageURL(discoveryURL, page, cfg.DiscoveryPerPage)
		if err != nil {
			return nil, err
		}
		pageCfg := cfg
		pageCfg.SearchURL = url
		body, status, err := fetchSearch(ctx, client, pageCfg)
		if err != nil {
			return nil, err
		}
		if status < 200 || status >= 300 {
			return nil, fmt.Errorf("discovery status %d: %s", status, truncate(string(body), 500))
		}
		pageIDs, err := extractDiscoveryIDs(body)
		if err != nil {
			return nil, err
		}
		ids = append(ids, pageIDs...)
	}
	return compactUnique(ids, cfg.DiscoveryPages*cfg.DiscoveryPerPage), nil
}

func fetchReleasePageResults(ctx context.Context, client *http.Client, cfg Config) ([]Result, error) {
	var out []Result
	for _, pageURL := range cfg.ReleasePageURLs {
		body, status, err := fetchURL(ctx, client, cfg, pageURL)
		if err != nil {
			return nil, err
		}
		if status < 200 || status >= 300 {
			return nil, fmt.Errorf("release page status %d: %s", status, truncate(string(body), 500))
		}
		source := discoverySourceKey(pageURL)
		ids, cards := extractReleasePage(string(body), pageURL)
		if len(ids) > 0 {
			productsCfg := cfg
			if productsCfg.ProductsManyURL == "" {
				productsCfg.ProductsManyURL = "https://api.solebox.com/sni-pl-prd-stor-we-char/v1/v1/products/many"
			}
			products, err := fetchProductBatches(ctx, client, productsCfg, ids)
			if err != nil {
				return nil, err
			}
			for i := range products {
				products[i].DiscoverySource = source
			}
			out = append(out, products...)
		}
		for i := range cards {
			cards[i].DiscoverySource = source
		}
		out = append(out, cards...)
	}
	return out, nil
}

func fetchProductBatches(ctx context.Context, client *http.Client, cfg Config, ids []string) ([]Result, error) {
	ids = compactUnique(ids, 10000)
	results := make([]Result, 0, len(ids))
	for start := 0; start < len(ids); start += cfg.BatchSize {
		end := min(start+cfg.BatchSize, len(ids))
		url, err := productsManyURL(cfg.ProductsManyURL, ids[start:end])
		if err != nil {
			return nil, err
		}
		batchCfg := cfg
		batchCfg.SearchURL = url
		body, status, err := fetchSearch(ctx, client, batchCfg)
		if err != nil {
			return nil, err
		}
		if status < 200 || status >= 300 {
			return nil, fmt.Errorf("products/many status %d: %s", status, truncate(string(body), 500))
		}
		batch, err := extractProductResults(body, cfg.ResultBaseURL)
		if err != nil {
			return nil, err
		}
		results = append(results, batch...)
	}
	return results, nil
}

func productsManyURL(raw string, ids []string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	query := parsed.Query()
	query.Set("ids", strings.Join(ids, ","))
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func discoveryPageURL(raw string, page int, perPage int) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	query := parsed.Query()
	query.Set("page", strconv.Itoa(page))
	query.Set("perPage", strconv.Itoa(perPage))
	if query.Get("sorting") == "" {
		query.Set("sorting", "new-desc")
	}
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func discoverySourceKey(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String()
}

func defaultProductsManyURL(discoveryURL string) string {
	parsed, err := url.Parse(discoveryURL)
	if err != nil {
		return ""
	}
	return parsed.Scheme + "://" + parsed.Host + "/sni-pl-prd-stor-we-char/v1/v1/products/many"
}

func idsFromURL(raw string) []string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil
	}
	return splitCSV(parsed.Query().Get("ids"))
}

func resultIDs(results []Result) []string {
	var ids []string
	for _, result := range results {
		if result.Snapshot != nil && result.Snapshot.ID != "" {
			ids = append(ids, result.Snapshot.ID)
			continue
		}
		if obj, ok := result.Raw.(map[string]any); ok {
			if id := firstDeepString(obj, "id", "productId", "masterId"); id != "" {
				ids = append(ids, id)
				continue
			}
		}
		if strings.HasPrefix(result.Key, "id:") {
			ids = append(ids, strings.TrimPrefix(result.Key, "id:"))
		}
	}
	return compactUnique(ids, 10000)
}

func dedupeResults(results []Result) []Result {
	seen := make(map[string]bool)
	out := make([]Result, 0, len(results))
	for _, result := range results {
		if result.Key == "" || seen[result.Key] {
			continue
		}
		seen[result.Key] = true
		out = append(out, result)
	}
	return out
}

func mergeResults(primary []Result, stock []Result) []Result {
	byID := make(map[string]Result)
	for _, result := range stock {
		if result.Snapshot != nil && result.Snapshot.ID != "" {
			byID[result.Snapshot.ID] = result
		}
	}
	out := make([]Result, 0, len(primary)+len(stock))
	used := make(map[string]bool)
	for _, result := range primary {
		id := ""
		if obj, ok := result.Raw.(map[string]any); ok {
			id = firstDeepString(obj, "id", "productId", "masterId")
		}
		if stockResult, ok := byID[id]; ok {
			out = append(out, stockResult)
			used[stockResult.Key] = true
			continue
		}
		out = append(out, result)
	}
	for _, result := range stock {
		if !used[result.Key] {
			out = append(out, result)
		}
	}
	return out
}

func productFingerprint(snapshot ProductSnapshot) string {
	type stockOnly struct {
		InStock    bool
		TotalStock int
		Status     string
		Sizes      []SizeStock
		Price      string
	}
	return stableJSONHash(stockOnly{
		InStock:    snapshot.InStock,
		TotalStock: snapshot.TotalStock,
		Status:     snapshot.Status,
		Sizes:      snapshot.Sizes,
		Price:      snapshot.Price,
	})
}

func productMetadataFingerprint(snapshot ProductSnapshot) string {
	type metadataOnly struct {
		Title  string
		Link   string
		Image  string
		Price  string
		Retail string
		SKU    string
		Brand  string
		Color  string
		Status string
	}
	return stableJSONHash(metadataOnly{
		Title:  snapshot.Title,
		Link:   snapshot.Link,
		Image:  snapshot.Image,
		Price:  snapshot.Price,
		Retail: snapshot.Retail,
		SKU:    snapshot.SKU,
		Brand:  snapshot.Brand,
		Color:  snapshot.Color,
		Status: snapshot.Status,
	})
}

func shouldAlertStockChange(previous ProductSnapshot, current ProductSnapshot, minDelta int) bool {
	if !previous.InStock && current.InStock {
		return true
	}
	if !current.InStock {
		return false
	}
	delta := current.TotalStock - previous.TotalStock
	if minDelta > 0 {
		return delta >= minDelta
	}
	if current.InStock && previous.TotalStock != current.TotalStock {
		return true
	}
	return current.InStock && sizeStockLine(previous.Sizes) != sizeStockLine(current.Sizes)
}

func stockChangeReason(previous ProductSnapshot, current ProductSnapshot) string {
	switch {
	case !previous.InStock && current.InStock:
		return "Restock"
	case previous.InStock && !current.InStock:
		return "Sold out"
	case previous.TotalStock != current.TotalStock:
		return fmt.Sprintf("Stock changed %d -> %d", previous.TotalStock, current.TotalStock)
	default:
		return "Product changed"
	}
}

func shouldAlertNewProduct(state *MonitorState, result Result, hadProductState bool, snapshot ProductSnapshot) bool {
	if !snapshot.InStock {
		return false
	}
	if result.DiscoverySource == "" {
		return hadProductState
	}
	source := state.Discoveries[result.DiscoverySource]
	if len(source) == 0 {
		return false
	}
	return !source[snapshot.Key]
}

func shouldAlertGenericResult(state *MonitorState, result Result) bool {
	if result.DiscoverySource == "" {
		return !state.Seen[result.Key]
	}
	source := state.Discoveries[result.DiscoverySource]
	if len(source) == 0 {
		return false
	}
	return !source[result.Key]
}

func markDiscoveryBaselines(state *MonitorState, results []Result) bool {
	changed := false
	if state.Discoveries == nil {
		state.Discoveries = make(map[string]map[string]bool)
		changed = true
	}
	for _, result := range results {
		if result.DiscoverySource == "" {
			continue
		}
		source := state.Discoveries[result.DiscoverySource]
		if source == nil {
			source = make(map[string]bool)
			state.Discoveries[result.DiscoverySource] = source
			changed = true
		}
		key := result.Key
		if result.Snapshot != nil {
			key = result.Snapshot.Key
		}
		if key != "" && !source[key] {
			source[key] = true
			changed = true
		}
	}
	return changed
}

func extractResults(body []byte, baseURL string) ([]Result, error) {
	var data any
	if err := json.Unmarshal(body, &data); err != nil {
		hash := sha256.Sum256(body)
		return []Result{{Key: hex.EncodeToString(hash[:]), Title: "Search response changed"}}, nil
	}

	var objects []map[string]any
	collectObjects(data, &objects)
	results := make([]Result, 0, len(objects))
	seenKeys := make(map[string]bool)

	for _, obj := range objects {
		key := resultKey(obj)
		if key == "" {
			key = stableJSONHash(obj)
		}
		if key == "" || seenKeys[key] {
			continue
		}
		seenKeys[key] = true
		results = append(results, Result{
			Key:   key,
			Link:  normalizeLink(firstString(obj, "url", "href", "link", "productUrl", "pdpUrl", "path", "slug"), baseURL),
			Title: firstString(obj, "name", "title", "displayName", "productName", "slug", "id", "sku"),
			Raw:   obj,
		})
	}

	if len(results) == 0 {
		key := stableJSONHash(data)
		if key != "" {
			results = append(results, Result{Key: key, Title: "Search response changed", Raw: data})
		}
	}
	return results, nil
}

func extractProductResults(body []byte, baseURL string) ([]Result, error) {
	var data any
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, err
	}
	var objects []map[string]any
	collectProductObjects(data, &objects)
	results := make([]Result, 0, len(objects))
	seenKeys := make(map[string]bool)
	for _, obj := range objects {
		snapshot := productSnapshot(obj, baseURL)
		if snapshot.Key == "" || seenKeys[snapshot.Key] {
			continue
		}
		seenKeys[snapshot.Key] = true
		results = append(results, Result{
			Key:      snapshot.Key,
			Link:     snapshot.Link,
			Title:    snapshot.Title,
			Raw:      obj,
			Snapshot: &snapshot,
		})
	}
	return results, nil
}

func extractDiscoveryIDs(body []byte) ([]string, error) {
	var data any
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, err
	}
	var ids []string
	if root, ok := data.(map[string]any); ok {
		for _, key := range []string{"products", "results", "items"} {
			if list, ok := root[key].([]any); ok {
				for _, item := range list {
					if obj, ok := item.(map[string]any); ok {
						if id := productObjectID(obj); id != "" {
							ids = append(ids, id)
						}
					}
				}
			}
		}
	}
	if len(ids) == 0 {
		collectDiscoveryIDs(data, &ids)
	}
	return compactUnique(ids, 10000), nil
}

func collectDiscoveryIDs(value any, out *[]string) {
	switch typed := value.(type) {
	case map[string]any:
		if id := productObjectID(typed); id != "" {
			*out = append(*out, id)
			return
		}
		for _, child := range typed {
			collectDiscoveryIDs(child, out)
		}
	case []any:
		for _, child := range typed {
			collectDiscoveryIDs(child, out)
		}
	}
}

func productObjectID(obj map[string]any) string {
	id := firstString(obj, "id", "productId", "masterId")
	if id == "" {
		return ""
	}
	if link := firstString(obj, "url", "href", "path", "productUrl", "pdpUrl"); strings.Contains(link, "/p/") {
		return id
	}
	if firstString(obj, "displayName", "name", "title", "productName") != "" && (obj["stock"] != nil || obj["firstLiveAt"] != nil || obj["isSoldOut"] != nil) {
		return id
	}
	return ""
}

func extractReleasePage(html string, pageURL string) ([]string, []Result) {
	articleRe := regexp.MustCompile(`(?is)<article\b[^>]*class="[^"]*\bitem\b[^"]*"[^>]*>.*?</article>`)
	idRe := regexp.MustCompile(`(?i)/p/[^"' <]+-(\d+)(?:[?#"' <]|$)`)
	var ids []string
	var cards []Result
	for _, article := range articleRe.FindAllString(html, -1) {
		if match := idRe.FindStringSubmatch(article); len(match) > 1 {
			ids = append(ids, match[1])
			continue
		}
		card := releaseCardFromArticle(article, pageURL)
		if card.Key != "" {
			cards = append(cards, card)
		}
	}
	return compactUnique(ids, 10000), cards
}

func releaseCardFromArticle(article string, pageURL string) Result {
	field := func(class string) string {
		re := regexp.MustCompile(`(?is)<div\b[^>]*class="[^"]*\b` + regexp.QuoteMeta(class) + `\b[^"]*"[^>]*>(.*?)</div>`)
		match := re.FindStringSubmatch(article)
		if len(match) < 2 {
			return ""
		}
		return cleanHTMLText(match[1])
	}
	image := firstRegexSubmatch(article, `(?is)<img\b[^>]*\bsrc="([^"]+)"`)
	brand := field("brand")
	name := field("name")
	price := field("price")
	date := field("date")
	status := field("status")
	if brand == "" && name == "" {
		return Result{}
	}
	title := strings.TrimSpace(strings.Join(compactUnique([]string{brand, name}, 2), " "))
	keyMaterial := strings.Join([]string{"release", brand, name, price, date, status, image}, "|")
	return Result{
		Key:    "release:" + shortHash(keyMaterial),
		Link:   pageURL,
		Title:  title,
		Reason: "New release card",
		Raw: map[string]any{
			"brand":  brand,
			"name":   name,
			"price":  price,
			"date":   date,
			"status": status,
			"image":  image,
			"url":    pageURL,
		},
	}
}

func firstRegexSubmatch(value string, pattern string) string {
	re := regexp.MustCompile(pattern)
	match := re.FindStringSubmatch(value)
	if len(match) < 2 {
		return ""
	}
	return html.UnescapeString(strings.TrimSpace(match[1]))
}

func cleanHTMLText(value string) string {
	tagRe := regexp.MustCompile(`(?is)<[^>]+>`)
	spaceRe := regexp.MustCompile(`\s+`)
	value = tagRe.ReplaceAllString(value, " ")
	value = html.UnescapeString(value)
	return strings.TrimSpace(spaceRe.ReplaceAllString(value, " "))
}

func collectProductObjects(value any, out *[]map[string]any) {
	switch typed := value.(type) {
	case map[string]any:
		if looksLikeProduct(typed) {
			*out = append(*out, typed)
			return
		}
		for _, child := range typed {
			collectProductObjects(child, out)
		}
	case []any:
		for _, child := range typed {
			collectProductObjects(child, out)
		}
	}
}

func looksLikeProduct(obj map[string]any) bool {
	if firstString(obj, "id", "productId", "masterId", "sku", "ean", "gtin") == "" {
		return false
	}
	if firstString(obj, "name", "title", "displayName", "productName", "slug", "url", "href", "path") != "" {
		return true
	}
	return firstDeepString(obj, "stock", "quantity", "available", "availability", "stockStatus") != ""
}

func productSnapshot(obj map[string]any, baseURL string) ProductSnapshot {
	id := firstString(obj, "id", "productId", "masterId")
	if id == "" {
		id = firstDeepString(obj, "id", "productId", "masterId")
	}
	key := "id:" + id
	if id == "" {
		key = resultKey(obj)
	}
	link := normalizeLink(firstDeepString(obj, "url", "href", "link", "productUrl", "pdpUrl", "path", "slug"), baseURL)
	sizes := extractSizeStock(obj)
	total := 0
	inStock := false
	for _, size := range sizes {
		total += size.Stock
		if size.InStock || size.Stock > 0 {
			inStock = true
		}
	}
	if total == 0 {
		total = firstInt(obj, "stock")
	}
	if total > 0 {
		inStock = true
	}
	status := firstDeepString(obj, "availability", "stockStatus", "status", "saleStatus")
	if !inStock {
		inStock = truthyStockStatus(status) || firstDeepBool(obj, "inStock", "isInStock", "available", "isAvailable")
	}
	if soldOut, ok := parseBool(obj["isSoldOut"]); ok && soldOut {
		inStock = false
	}
	snapshot := ProductSnapshot{
		ID:         id,
		Key:        key,
		Title:      firstNonEmpty(firstString(obj, "name", "title", "displayName", "productName"), firstDeepString(obj, "name", "title", "displayName", "productName")),
		Link:       link,
		Image:      firstNonEmpty(normalizeLink(firstDeepString(obj, "image", "imageUrl", "image_url", "thumbnail", "thumbnailUrl", "src"), ""), soleboxImageURL(obj, link)),
		Price:      firstDeepString(obj, "price", "currentPrice", "salePrice", "finalPrice", "formattedPrice"),
		Retail:     firstDeepString(obj, "retailPrice", "regularPrice", "originalPrice", "msrp", "wasPrice", "highestRetailPrice"),
		SKU:        firstNonEmpty(soleboxProductAttribute(obj, "manufacturerCode"), soleboxProductAttribute(obj, "productCode"), soleboxProductAttribute(obj, "articleNo"), firstDeepString(obj, "sku", "styleCode", "style", "ean", "gtin")),
		Brand:      firstNonEmpty(soleboxProductAttribute(obj, "brand"), firstDeepString(obj, "brand", "manufacturer", "vendor")),
		Color:      firstNonEmpty(soleboxProductAttribute(obj, "color"), firstDeepString(obj, "color", "colorway", "colour")),
		Status:     status,
		InStock:    inStock,
		TotalStock: total,
		Sizes:      sizes,
	}
	snapshot.Fingerprint = productFingerprint(snapshot)
	return snapshot
}

func extractSizeStock(value any) []SizeStock {
	var sizes []SizeStock
	collectSizeStock(value, &sizes)
	sort.SliceStable(sizes, func(i, j int) bool {
		return sizes[i].Label < sizes[j].Label
	})
	return compactSizeStock(sizes)
}

func collectSizeStock(value any, out *[]SizeStock) {
	switch typed := value.(type) {
	case map[string]any:
		if size, ok := soleboxVariantSizeStock(typed); ok {
			*out = append(*out, size)
			return
		}
		label := firstString(typed, "size", "label", "value", "eu", "us", "uk")
		stock := firstInt(typed, "stock", "quantity", "qty", "availableQuantity", "availability")
		inStock := stock > 0 || firstBool(typed, "inStock", "isInStock", "available", "isAvailable")
		if label != "" && (stock > 0 || inStock || hasAnyKey(typed, "stock", "quantity", "qty", "availableQuantity", "availability")) {
			*out = append(*out, SizeStock{Label: label, Stock: stock, InStock: inStock})
			return
		}
		for _, child := range typed {
			collectSizeStock(child, out)
		}
	case []any:
		for _, child := range typed {
			collectSizeStock(child, out)
		}
	}
}

func soleboxVariantSizeStock(obj map[string]any) (SizeStock, bool) {
	stockObj, ok := obj["stock"].(map[string]any)
	if !ok {
		return SizeStock{}, false
	}
	attrs, ok := obj["attributes"].(map[string]any)
	if !ok {
		return SizeStock{}, false
	}
	label := soleboxAttributeValue(attrs, "sizeEu", "size", "sizeUs", "sizeUk")
	if label == "" {
		return SizeStock{}, false
	}
	quantity := firstInt(stockObj, "quantity")
	inStock := quantity > 0 || firstBool(stockObj, "isSellableWithoutStock")
	return SizeStock{Label: label, Stock: quantity, InStock: inStock}, true
}

func soleboxAttributeValue(attrs map[string]any, names ...string) string {
	for _, name := range names {
		attr, ok := attrs[name].(map[string]any)
		if !ok {
			continue
		}
		values, ok := attr["values"].(map[string]any)
		if !ok {
			continue
		}
		if label := firstString(values, "label", "value"); label != "" {
			return label
		}
	}
	return ""
}

func soleboxProductAttribute(obj map[string]any, name string) string {
	attrs, ok := obj["attributes"].(map[string]any)
	if !ok {
		return ""
	}
	return soleboxAttributeValue(attrs, name)
}

func soleboxImageURL(obj map[string]any, productLink string) string {
	images, ok := obj["images"].([]any)
	if !ok || len(images) == 0 {
		return ""
	}
	first, ok := images[0].(map[string]any)
	if !ok {
		return ""
	}
	publicID := firstString(first, "public_id", "publicid", "publicId")
	if publicID == "" {
		return ""
	}
	if strings.Contains(strings.ToLower(publicID), "fallback") ||
		strings.Contains(strings.ToLower(publicID), "placeholder") {
		return ""
	}
	base := "https://asset.solebox.com/images/f_auto,q_80,d_fallback-sole.png/b_rgb:f8f8f8,c_pad,w_680,h_680/dpr_1.0/" + url.PathEscape(publicID)
	if slug := soleboxProductSlug(productLink); slug != "" {
		return base + "/" + url.PathEscape(slug+"-1")
	}
	return base
}

func soleboxProductSlug(productLink string) string {
	parsed, err := url.Parse(strings.TrimSpace(productLink))
	if err != nil {
		return ""
	}
	path := strings.Trim(parsed.Path, "/")
	if path == "" {
		return ""
	}
	parts := strings.Split(path, "/")
	for i, part := range parts {
		if part == "p" && i+1 < len(parts) {
			return strings.TrimSpace(parts[i+1])
		}
	}
	if len(parts) == 1 {
		return strings.TrimSpace(parts[0])
	}
	return ""
}

func compactSizeStock(values []SizeStock) []SizeStock {
	byLabel := make(map[string]SizeStock)
	for _, value := range values {
		label := strings.TrimSpace(value.Label)
		if label == "" {
			continue
		}
		current := byLabel[label]
		current.Label = label
		current.Stock += value.Stock
		current.InStock = current.InStock || value.InStock || value.Stock > 0
		byLabel[label] = current
	}
	out := make([]SizeStock, 0, len(byLabel))
	for _, value := range byLabel {
		out = append(out, value)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Label < out[j].Label
	})
	return out
}

func collectObjects(value any, out *[]map[string]any) {
	switch typed := value.(type) {
	case map[string]any:
		if looksLikeResult(typed) {
			*out = append(*out, typed)
		}
		for _, child := range typed {
			collectObjects(child, out)
		}
	case []any:
		for _, child := range typed {
			collectObjects(child, out)
		}
	}
}

func looksLikeResult(obj map[string]any) bool {
	link := firstString(obj, "url", "href", "link", "productUrl", "pdpUrl", "path")
	if link != "" {
		return true
	}
	slug := firstString(obj, "slug")
	title := firstString(obj, "name", "title", "displayName", "productName")
	sku := firstString(obj, "sku", "ean", "gtin")
	return slug != "" && (title != "" || sku != "")
}

func resultKey(obj map[string]any) string {
	for _, key := range []string{"url", "href", "link", "productUrl", "pdpUrl", "path", "slug", "sku", "ean", "gtin", "id"} {
		if value := stringify(obj[key]); value != "" {
			return key + ":" + value
		}
	}
	return ""
}

func firstString(obj map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := stringify(obj[key]); value != "" {
			return value
		}
	}
	return ""
}

func firstDeepString(value any, keys ...string) string {
	keySet := make(map[string]bool, len(keys))
	for _, key := range keys {
		keySet[strings.ToLower(key)] = true
	}
	return firstDeepStringFromSet(value, keySet)
}

func firstDeepStringFromSet(value any, keys map[string]bool) string {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if keys[strings.ToLower(key)] {
				if value := stringify(child); value != "" {
					return value
				}
			}
		}
		for _, child := range typed {
			if value := firstDeepStringFromSet(child, keys); value != "" {
				return value
			}
		}
	case []any:
		for _, child := range typed {
			if value := firstDeepStringFromSet(child, keys); value != "" {
				return value
			}
		}
	}
	return ""
}

func firstDeepBool(value any, keys ...string) bool {
	keySet := make(map[string]bool, len(keys))
	for _, key := range keys {
		keySet[strings.ToLower(key)] = true
	}
	return firstDeepBoolFromSet(value, keySet)
}

func firstDeepBoolFromSet(value any, keys map[string]bool) bool {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if keys[strings.ToLower(key)] {
				if value, ok := parseBool(child); ok {
					return value
				}
			}
		}
		for _, child := range typed {
			if firstDeepBoolFromSet(child, keys) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if firstDeepBoolFromSet(child, keys) {
				return true
			}
		}
	}
	return false
}

func firstDeepStringList(value any, keys ...string) []string {
	keySet := make(map[string]bool, len(keys))
	for _, key := range keys {
		keySet[strings.ToLower(key)] = true
	}

	var values []string
	collectDeepStrings(value, keySet, &values)
	return compactUnique(values, 20)
}

func collectDeepStrings(value any, keys map[string]bool, out *[]string) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if keys[strings.ToLower(key)] {
				if value := stringify(child); value != "" {
					*out = append(*out, value)
				}
			}
		}
		for _, child := range typed {
			collectDeepStrings(child, keys, out)
		}
	case []any:
		for _, child := range typed {
			collectDeepStrings(child, keys, out)
		}
	}
}

func compactUnique(values []string, limit int) []string {
	seen := make(map[string]bool)
	var out []string
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
		if len(out) >= limit {
			break
		}
	}
	return out
}

func stringify(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case float64:
		if typed == float64(int64(typed)) {
			return strconv.FormatInt(int64(typed), 10)
		}
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(typed)
	default:
		return ""
	}
}

func firstInt(obj map[string]any, keys ...string) int {
	for _, key := range keys {
		if value, ok := parseInt(obj[key]); ok {
			return value
		}
	}
	return 0
}

func firstBool(obj map[string]any, keys ...string) bool {
	for _, key := range keys {
		if value, ok := parseBool(obj[key]); ok {
			return value
		}
	}
	return false
}

func parseInt(value any) (int, bool) {
	switch typed := value.(type) {
	case float64:
		return int(typed), true
	case int:
		return typed, true
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(typed))
		return parsed, err == nil
	default:
		return 0, false
	}
}

func parseBool(value any) (bool, bool) {
	switch typed := value.(type) {
	case bool:
		return typed, true
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "true", "yes", "available", "instock", "in_stock", "in stock", "sellable", "online":
			return true, true
		case "false", "no", "unavailable", "outofstock", "out_of_stock", "out of stock", "soldout", "sold out":
			return false, true
		}
	case float64:
		return typed > 0, true
	}
	return false, false
}

func hasAnyKey(obj map[string]any, keys ...string) bool {
	for _, key := range keys {
		if _, ok := obj[key]; ok {
			return true
		}
	}
	return false
}

func truthyStockStatus(status string) bool {
	status = strings.ToLower(strings.TrimSpace(status))
	return status == "available" || status == "instock" || status == "in_stock" || status == "in stock" || status == "sellable" || status == "online"
}

func normalizeLink(raw string, baseURL string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if parsed, err := url.Parse(raw); err == nil && parsed.IsAbs() {
		return raw
	}
	if baseURL == "" {
		return raw
	}
	if strings.HasPrefix(raw, "/") {
		return baseURL + raw
	}
	return baseURL + "/" + raw
}

func sendDiscordBatches(ctx context.Context, client *http.Client, cfg Config, results []Result) error {
	for i := 0; i < len(results); i += 10 {
		end := min(i+10, len(results))
		message := DiscordMessage{
			Content:  discordContent(cfg.DiscordMention, i, len(results)),
			Username: cfg.DiscordUsername,
			Embeds:   discordEmbeds(results[i:end]),
		}
		if err := postDiscordWithRetry(ctx, client, cfg.WebhookURL, message); err != nil {
			return err
		}
		time.Sleep(1200 * time.Millisecond)
	}
	return nil
}

func discordContent(mention string, offset int, total int) string {
	if mention == "" {
		if offset == 0 {
			return fmt.Sprintf("Solebox alerts: %d", total)
		}
		return ""
	}
	if offset == 0 {
		return fmt.Sprintf("%s Solebox alerts: %d", mention, total)
	}
	return mention
}

func discordEmbeds(results []Result) []DiscordEmbed {
	embeds := make([]DiscordEmbed, 0, len(results))
	for _, result := range results {
		embeds = append(embeds, discordEmbed(result))
	}
	return embeds
}

func discordEmbed(result Result) DiscordEmbed {
	obj, _ := result.Raw.(map[string]any)
	title := discordTitle(result)
	if title == "" {
		title = "New product"
	}

	embed := DiscordEmbed{
		Title:     truncate(title, 250),
		URL:       result.Link,
		Color:     discordColor(result),
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Footer:    &DiscordEmbedFooter{Text: "Solebox Monitor"},
	}

	if image := normalizeLink(firstDeepString(obj, "image", "imageUrl", "image_url", "thumbnail", "thumbnailUrl", "src"), ""); image != "" {
		embed.Thumbnail = &DiscordEmbedImage{URL: image}
	}

	addField := func(name string, value string, inline bool) {
		value = truncate(strings.TrimSpace(value), 1024)
		if value != "" && value != "0" && len(embed.Fields) < 25 {
			embed.Fields = append(embed.Fields, DiscordEmbedField{Name: name, Value: value, Inline: inline})
		}
	}

	if result.Reason != "" {
		addField("Type", result.Reason, true)
	}
	if result.Snapshot != nil {
		snapshot := result.Snapshot
		if snapshot.Image != "" {
			embed.Thumbnail = &DiscordEmbedImage{URL: snapshot.Image}
			embed.Image = &DiscordEmbedImage{URL: snapshot.Image}
		}
		addField("Price", snapshot.Price, true)
		addField("Retail", cleanRetail(snapshot.Retail), true)
		addField("SKU", snapshot.SKU, true)
		addField("Brand", snapshot.Brand, true)
		addField("Color", snapshot.Color, true)
		addField("Stock", stockSummary(snapshot), true)
		addField("Sizes", sizeStockLine(snapshot.Sizes), false)
	} else {
		addField("Price", firstDeepString(obj, "price", "currentPrice", "salePrice", "finalPrice", "formattedPrice"), true)
		addField("Retail", cleanRetail(firstDeepString(obj, "retailPrice", "regularPrice", "originalPrice", "msrp")), true)
		addField("SKU", firstDeepString(obj, "sku", "styleCode", "style", "ean", "gtin"), true)
		addField("Brand", firstDeepString(obj, "brand", "manufacturer", "vendor"), true)
		addField("Name", firstDeepString(obj, "name", "title", "productName"), true)
		addField("Status", firstDeepString(obj, "availability", "stockStatus", "status"), true)
		addField("Date", firstDeepString(obj, "date"), true)
		addField("Sizes", strings.Join(firstDeepStringList(obj, "size", "sizes", "label", "eu", "us"), ", "), false)
	}

	if result.Link != "" {
		addField("Open", result.Link, false)
	}
	if len(embed.Fields) == 0 {
		embed.Description = displayResult(result)
	}

	return embed
}

func discordTitle(result Result) string {
	if result.Snapshot == nil {
		return result.Title
	}
	switch result.Reason {
	case "New product":
		return "NEW: " + result.Snapshot.Title
	case "Restock":
		return "RESTOCK: " + result.Snapshot.Title
	default:
		if strings.HasPrefix(result.Reason, "Stock changed") {
			return "STOCK UPDATE: " + result.Snapshot.Title
		}
		return result.Snapshot.Title
	}
}

func discordColor(result Result) int {
	switch {
	case result.Reason == "New product":
		return 0x2ecc71
	case result.Reason == "Restock":
		return 0x3498db
	case strings.Contains(strings.ToLower(result.Reason), "release"):
		return 0xf1c40f
	case strings.HasPrefix(result.Reason, "Stock changed"):
		return 0x95a5a6
	default:
		return 0x111111
	}
}

func cleanRetail(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || value == "0,00 €" || value == "0,00 €" || value == "0" {
		return ""
	}
	return value
}

func stockSummary(snapshot *ProductSnapshot) string {
	if snapshot == nil {
		return ""
	}
	if !snapshot.InStock {
		return "Sold out"
	}
	if snapshot.TotalStock > 0 {
		return fmt.Sprintf("%d available", snapshot.TotalStock)
	}
	return "Available"
}

func postDiscordWithRetry(ctx context.Context, client *http.Client, webhookURL string, message DiscordMessage) error {
	var lastErr error
	for attempt := 0; attempt < 6; attempt++ {
		if attempt > 0 {
			log.Printf("discord retry %d after rate limit", attempt)
		}
		retryAfter, err := postDiscord(ctx, client, webhookURL, message)
		if err == nil {
			return nil
		}
		lastErr = err
		if retryAfter <= 0 {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryAfter):
		}
	}
	return lastErr
}

func postDiscord(ctx context.Context, client *http.Client, webhookURL string, message DiscordMessage) (time.Duration, error) {
	payload, err := json.Marshal(message)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, bytes.NewReader(payload))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if resp.StatusCode == http.StatusTooManyRequests {
			return discordRetryAfter(resp, body), fmt.Errorf("discord status %d: %s", resp.StatusCode, truncate(string(body), 500))
		}
		return 0, fmt.Errorf("discord status %d: %s", resp.StatusCode, truncate(string(body), 500))
	}
	return 0, nil
}

func discordRetryAfter(resp *http.Response, body []byte) time.Duration {
	var rateLimit DiscordRateLimit
	if err := json.Unmarshal(body, &rateLimit); err == nil && rateLimit.RetryAfter > 0 {
		return time.Duration(rateLimit.RetryAfter*1000) * time.Millisecond
	}
	if header := strings.TrimSpace(resp.Header.Get("Retry-After")); header != "" {
		if seconds, err := strconv.ParseFloat(header, 64); err == nil && seconds > 0 {
			return time.Duration(seconds*1000) * time.Millisecond
		}
	}
	return 2 * time.Second
}

func loadState(path string) (*MonitorState, error) {
	emptyState := func() *MonitorState {
		return &MonitorState{
			Seen:        make(map[string]bool),
			Products:    make(map[string]ProductSnapshot),
			Discoveries: make(map[string]map[string]bool),
		}
	}
	state := emptyState()
	body, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return state, nil
	}
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return state, nil
	}
	var current MonitorState
	if err := json.Unmarshal(body, &current); err == nil && (current.Seen != nil || current.Products != nil) {
		if current.Seen == nil {
			current.Seen = make(map[string]bool)
		}
		if current.Products == nil {
			current.Products = make(map[string]ProductSnapshot)
		}
		if current.Discoveries == nil {
			current.Discoveries = make(map[string]map[string]bool)
		}
		return &current, nil
	}

	legacy := make(map[string]bool)
	if err := json.Unmarshal(body, &legacy); err != nil {
		return nil, err
	}
	state = emptyState()
	state.Seen = legacy
	return state, nil
}

func saveState(path string, state *MonitorState) error {
	state.Updated = time.Now().UTC().Format(time.RFC3339)
	body, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(body, '\n'), 0600)
}

func currentEnvKeys() map[string]bool {
	keys := make(map[string]bool)
	for _, pair := range os.Environ() {
		key, _, _ := strings.Cut(pair, "=")
		if key != "" {
			keys[key] = true
		}
	}
	return keys
}

func loadDotEnv(path string, protected map[string]bool) error {
	body, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		value = strings.Trim(value, `"'`)
		if key != "" {
			if protected[key] {
				continue
			}
			os.Setenv(key, value)
		}
	}
	return nil
}

func stableJSONHash(value any) string {
	canonical, err := json.Marshal(canonicalize(value))
	if err != nil {
		return ""
	}
	hash := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(hash[:])
}

func shortHash(value string) string {
	hash := sha256.Sum256([]byte(value))
	return hex.EncodeToString(hash[:])[:16]
}

func canonicalize(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		out := make([]any, 0, len(keys)*2)
		for _, key := range keys {
			out = append(out, key, canonicalize(typed[key]))
		}
		return out
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, canonicalize(item))
		}
		return out
	default:
		return typed
	}
}

func getenv(key string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func getenvInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func getenvBool(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, ok := parseBool(value)
	if !ok {
		return fallback
	}
	return parsed
}

func splitCSV(value string) []string {
	parts := strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == '\n' || r == ' ' || r == '\t'
	})
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return compactUnique(out, 10000)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func displayResult(result Result) string {
	if result.Link != "" {
		return result.Link
	}
	if result.Title != "" {
		return result.Title
	}
	return result.Key
}

func sizeStockLine(sizes []SizeStock) string {
	if len(sizes) == 0 {
		return ""
	}
	parts := make([]string, 0, len(sizes))
	for _, size := range sizes {
		if size.Stock > 0 {
			parts = append(parts, fmt.Sprintf("%s: %d", size.Label, size.Stock))
			continue
		}
		if size.InStock {
			parts = append(parts, size.Label+": available")
			continue
		}
		parts = append(parts, size.Label+": 0")
	}
	return strings.Join(parts, ", ")
}

func truncate(value string, max int) string {
	value = strings.TrimSpace(value)
	if len(value) <= max {
		return value
	}
	return value[:max] + "..."
}
