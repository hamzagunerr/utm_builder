package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
	"github.com/xuri/excelize/v2"
)

// Global bot instance for API handlers
var globalBot *tgbotapi.BotAPI
var db *bun.DB

// getEnv environment variable'dan değer alır, yoksa default değer döner
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getBotToken environment variable'dan bot token'ı alır
func getBotToken() string {
	token := os.Getenv("TELEGRAM_BOT_TOKEN")
	if token == "" {
		log.Fatal("TELEGRAM_BOT_TOKEN environment variable is not set")
	}
	return token
}

// getNotificationChatIDs bildirim gönderilecek chat ID'lerini alır (virgülle ayrılmış)
// Örnek: NOTIFICATION_CHAT_IDS=1026146458,-1001234567890
func getNotificationChatIDs() []int64 {
	chatIDsStr := os.Getenv("NOTIFICATION_CHAT_IDS")
	// Eski format desteği (tek ID)
	if chatIDsStr == "" {
		chatIDsStr = os.Getenv("NOTIFICATION_CHAT_ID")
	}
	if chatIDsStr == "" {
		log.Println("UYARI: NOTIFICATION_CHAT_IDS ayarlanmamış, bildirimler gönderilemeyecek")
		return nil
	}

	var chatIDs []int64
	parts := strings.Split(chatIDsStr, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		var chatID int64
		if _, err := fmt.Sscanf(part, "%d", &chatID); err == nil && chatID != 0 {
			chatIDs = append(chatIDs, chatID)
		}
	}

	if len(chatIDs) > 0 {
		log.Printf("Bildirimler %d hedefe gönderilecek: %v", len(chatIDs), chatIDs)
	}
	return chatIDs
}

// Order veritabanı modeli
type Order struct {
	bun.BaseModel `bun:"table:orders,alias:o"`

	ID             int64       `bun:"id,pk,autoincrement"`
	OrderID        string      `bun:"order_id,notnull,unique"`
	Amount         float64     `bun:"amount,notnull"`
	Currency       string      `bun:"currency,notnull"`
	Items          []OrderItem `bun:"items,type:jsonb"`
	UTMSource      string      `bun:"utm_source"`
	UTMMedium      string      `bun:"utm_medium"`
	UTMCampaign    string      `bun:"utm_campaign"`
	UTMContent     string      `bun:"utm_content"`
	UTMTerm        string      `bun:"utm_term"`
	GadSource      string      `bun:"gad_source"`
	GadCampaignID  string      `bun:"gad_campaignid"`
	TrafficChannel string      `bun:"traffic_channel"`
	EventTime      time.Time   `bun:"event_time,notnull"`
	CreatedAt      time.Time   `bun:"created_at,nullzero,notnull,default:current_timestamp"`
}

// OrderItem sipariş kalemi
type OrderItem struct {
	ItemID   string  `json:"item_id"`
	ItemName string  `json:"item_name"`
	Quantity int     `json:"quantity"`
	Price    float64 `json:"price"`
}

// ThrowDataRequest API isteği için struct
type ThrowDataRequest struct {
	OrderID        string      `json:"order_id"`
	Amount         float64     `json:"amount"`
	Currency       string      `json:"currency"`
	Items          []OrderItem `json:"items"`
	UTMSource      string      `json:"utm_source"`
	UTMMedium      string      `json:"utm_medium"`
	UTMCampaign    string      `json:"utm_campaign"`
	UTMContent     string      `json:"utm_content"`
	UTMTerm        string      `json:"utm_term"`
	GadSource      string      `json:"gad_source"`
	GadCampaignID  string      `json:"gad_campaignid"`
	TrafficChannel string      `json:"traffic_channel"`
	EventTime      time.Time   `json:"event_time"`
}

// initDatabase veritabanı bağlantısını başlatır
func initDatabase() error {
	dsn := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/utm_builder?sslmode=disable")

	sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn)))
	db = bun.NewDB(sqldb, pgdialect.New())

	// Bağlantıyı test et
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("veritabanına bağlanılamadı: %w", err)
	}

	log.Println("PostgreSQL veritabanına bağlandı")

	// Tabloları oluştur
	_, err := db.NewCreateTable().Model((*Order)(nil)).IfNotExists().Exec(ctx)
	if err != nil {
		return fmt.Errorf("tablo oluşturulamadı: %w", err)
	}

	// Yeni sütunları ekle (migration)
	migrations := []string{
		"ALTER TABLE orders ADD COLUMN IF NOT EXISTS utm_content VARCHAR(255)",
		"ALTER TABLE orders ADD COLUMN IF NOT EXISTS utm_term VARCHAR(255)",
		"ALTER TABLE orders ADD COLUMN IF NOT EXISTS gad_source VARCHAR(255)",
		"ALTER TABLE orders ADD COLUMN IF NOT EXISTS gad_campaignid VARCHAR(255)",
		"ALTER TABLE orders ADD COLUMN IF NOT EXISTS traffic_channel VARCHAR(255)",
	}

	for _, migration := range migrations {
		if _, err := db.ExecContext(ctx, migration); err != nil {
			log.Printf("Migration uyarı (muhtemelen sütun zaten var): %v", err)
		}
	}

	log.Println("Veritabanı tabloları hazır")
	return nil
}

// startFiberServer Fiber HTTP server'ı başlatır
func startFiberServer() {
	app := fiber.New(fiber.Config{
		AppName: "UTM Builder Bot API",
	})

	app.Use(func(c *fiber.Ctx) error {
		if c.Method() == "OPTIONS" {
			return c.Next()
		}

		return logger.New(logger.Config{
			Format:     "${method} ${path} - ${status} - ${latency}\n",
			TimeFormat: "02-Jan-2006 15:04:05",
			TimeZone:   "Local",
		})(c)
	})

	app.Use(cors.New(cors.Config{
		AllowOriginsFunc: func(origin string) bool {
			if origin == "http://localhost:3061" || origin == "https://www.hayratyardim.org" || origin == "https://hayratyardim.org" {
				return true
			} else {
				return false
			}
		},
		AllowCredentials: true,
		AllowMethods:     "GET,POST,PUT,DELETE,OPTIONS",
		AllowHeaders:     "Origin, Content-Type, Accept, Authorization, X-Requested-With, X-User-Uuid",
		AllowOrigins:     "http://localhost:3061",
	}))

	// Logger middleware
	app.Use(logger.New())

	// Health check endpoint
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": "ok"})
	})

	// Throw data endpoint
	app.Post("/throw-data", handleThrowData)

	port := getEnv("API_PORT", "3061")
	log.Printf("Fiber API sunucusu başlatılıyor: :%s", port)

	if err := app.Listen(":" + port); err != nil {
		log.Fatalf("Fiber sunucusu başlatılamadı: %v", err)
	}
}

// handleThrowData /throw-data endpoint handler'ı
func handleThrowData(c *fiber.Ctx) error {
	var req ThrowDataRequest

	if err := c.BodyParser(&req); err != nil {
		log.Printf("JSON parse hatası: %v", err)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Geçersiz JSON formatı",
		})
	}

	log.Printf("Yeni sipariş alındı: %s, Tutar: %.2f %s", req.OrderID, req.Amount, req.Currency)

	// Veritabanına kaydet
	order := &Order{
		OrderID:        req.OrderID,
		Amount:         req.Amount,
		Currency:       req.Currency,
		Items:          req.Items,
		UTMSource:      req.UTMSource,
		UTMMedium:      req.UTMMedium,
		UTMCampaign:    req.UTMCampaign,
		UTMContent:     req.UTMContent,
		UTMTerm:        req.UTMTerm,
		GadSource:      req.GadSource,
		GadCampaignID:  req.GadCampaignID,
		TrafficChannel: req.TrafficChannel,
		EventTime:      req.EventTime,
	}

	ctx := context.Background()
	_, err := db.NewInsert().Model(order).Exec(ctx)
	if err != nil {
		log.Printf("Veritabanı kayıt hatası: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Veritabanı hatası",
		})
	}

	// Telegram'a bildirim gönder (tüm hedeflere)
	chatIDs := getNotificationChatIDs()
	if len(chatIDs) > 0 && globalBot != nil {
		message := formatOrderMessage(&req)
		for _, chatID := range chatIDs {
			msg := tgbotapi.NewMessage(chatID, message)
			msg.ParseMode = "HTML"
			if _, err := globalBot.Send(msg); err != nil {
				log.Printf("Telegram mesaj gönderme hatası (chat_id=%d): %v", chatID, err)
			} else {
				log.Printf("Telegram bildirimi gönderildi: chat_id=%d", chatID)
			}
		}
	}

	return c.JSON(fiber.Map{
		"success": true,
		"message": "Veri başarıyla kaydedildi ve bildirim gönderildi",
	})
}

// formatOrderMessage siparişi okunabilir mesaja dönüştürür (HTML format)
func formatOrderMessage(req *ThrowDataRequest) string {
	var sb strings.Builder

	// Türkiye saati için UTC+3 ekle
	turkeyTime := req.EventTime.Add(3 * time.Hour)

	sb.WriteString("🛒 <b>Yeni Bağış Bildirimi</b>\n\n")
	sb.WriteString(fmt.Sprintf("📋 <b>Sipariş ID:</b> <code>%s</code>\n", req.OrderID))
	sb.WriteString(fmt.Sprintf("💰 <b>Tutar:</b> %.2f %s\n", req.Amount, req.Currency))
	sb.WriteString(fmt.Sprintf("📅 <b>Tarih:</b> %s\n\n", turkeyTime.Format("02.01.2006 15:04:05")))

	if len(req.Items) > 0 {
		sb.WriteString("📦 <b>Bağış Kalemleri:</b>\n")
		for _, item := range req.Items {
			sb.WriteString(fmt.Sprintf("  • %s (x%d) - %.2f %s\n", item.ItemName, item.Quantity, item.Price, req.Currency))
		}
		sb.WriteString("\n")
	}

	// UTM Bilgileri
	hasUTM := req.UTMSource != "" || req.UTMMedium != "" || req.UTMCampaign != "" || req.UTMContent != "" || req.UTMTerm != ""
	if hasUTM {
		sb.WriteString("📊 <b>UTM Bilgileri:</b>\n")
		if req.UTMSource != "" {
			sb.WriteString(fmt.Sprintf("  • Kaynak: %s\n", req.UTMSource))
		}
		if req.UTMMedium != "" {
			sb.WriteString(fmt.Sprintf("  • Ortam: %s\n", req.UTMMedium))
		}
		if req.UTMCampaign != "" {
			sb.WriteString(fmt.Sprintf("  • Kampanya: %s\n", req.UTMCampaign))
		}
		if req.UTMContent != "" {
			sb.WriteString(fmt.Sprintf("  • İçerik: %s\n", req.UTMContent))
		}
		if req.UTMTerm != "" {
			sb.WriteString(fmt.Sprintf("  • Terim: %s\n", req.UTMTerm))
		}
		sb.WriteString("\n")
	}

	// Google Ads Bilgileri
	hasGoogle := req.GadSource != "" || req.GadCampaignID != ""
	if hasGoogle {
		sb.WriteString("🔍 <b>Google Ads Bilgileri:</b>\n")
		if req.GadSource != "" {
			sb.WriteString(fmt.Sprintf("  • gad_source: %s\n", req.GadSource))
		}
		if req.GadCampaignID != "" {
			sb.WriteString(fmt.Sprintf("  • gad_campaignid: %s\n", req.GadCampaignID))
		}
		sb.WriteString("\n")
	}

	// Trafik Kanalı
	if req.TrafficChannel != "" {
		sb.WriteString(fmt.Sprintf("📡 <b>Trafik Kanalı:</b> %s\n", req.TrafficChannel))
	}

	return sb.String()
}

// UserSession kullanıcının UTM oluşturma sürecindeki durumunu tutar
type UserSession struct {
	Step      int    // Hangi adımda olduğu (1-6)
	SourceURL string // Kaynak URL
	UTMSource string // utm_source
	UTMMedium string // utm_medium
	Campaign  string // utm_campaign
	Content   string // utm_content
	Term      string // utm_term (opsiyonel)
}

// sessions tüm kullanıcı oturumlarını tutar
var sessions = make(map[int64]*UserSession)
var sessionsMutex sync.RWMutex

// UTM Source seçenekleri
var utmSourceOptions = []string{"meta", "google", "tiktok", "linkedin", "sms", "email", "x"}

// UTM Medium seçenekleri
var utmMediumOptions = []string{"paid_social", "cpc", "display", "paid_search", "sms", "email", "organic_social"}

func main() {
	// Veritabanını başlat
	if err := initDatabase(); err != nil {
		log.Printf("UYARI: Veritabanı başlatılamadı: %v", err)
		log.Println("Bot veritabanı olmadan çalışmaya devam edecek")
	}

	// Bot'u oluştur
	bot, err := tgbotapi.NewBotAPI(getBotToken())
	if err != nil {
		log.Panic(err)
	}

	// Global bot instance'ı ayarla (API handler'ları için)
	globalBot = bot

	bot.Debug = true // Debug modunu aç - sorun tespiti için
	log.Printf("Bot başlatıldı: @%s", bot.Self.UserName)

	// Fiber sunucusunu ayrı goroutine'de başlat
	go startFiberServer()

	// Update config
	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	updates := bot.GetUpdatesChan(u)

	for update := range updates {
		log.Printf("Update alındı: %+v", update)

		// Callback query (inline button tıklaması)
		if update.CallbackQuery != nil {
			log.Printf("Callback query: user=%d, data=%s", update.CallbackQuery.From.ID, update.CallbackQuery.Data)
			handleCallback(bot, update.CallbackQuery)
			continue
		}

		// Normal mesaj
		if update.Message != nil {
			log.Printf("Mesaj alındı: user=%d, text=%s", update.Message.From.ID, update.Message.Text)
			handleMessage(bot, update.Message)
		}
	}
}

// handleMessage normal mesajları işler
func handleMessage(bot *tgbotapi.BotAPI, message *tgbotapi.Message) {
	userID := message.From.ID
	chatID := message.Chat.ID

	// Komutları kontrol et
	if message.IsCommand() {
		log.Printf("Komut alındı: /%s, user=%d, chat=%d", message.Command(), userID, chatID)
		switch message.Command() {
		case "start":
			sendWelcomeMessage(bot, chatID)
		case "build":
			startBuildProcess(bot, chatID, userID)
		case "cancel":
			cancelSession(bot, chatID, userID)
		case "myid":
			sendMyID(bot, chatID, userID)
		case "toplam":
			handleToplamCommand(bot, chatID, message.CommandArguments())
		case "kaynaklar":
			handleKaynaklarCommand(bot, chatID, message.CommandArguments())
		case "kampanyalar":
			handleKampanyalarCommand(bot, chatID, message.CommandArguments())
		case "ortamlar":
			handleOrtamlarCommand(bot, chatID, message.CommandArguments())
		case "son":
			handleSonCommand(bot, chatID, message.CommandArguments())
		case "gunluk":
			handleGunlukCommand(bot, chatID)
		case "ortalama":
			handleOrtalamaCommand(bot, chatID, message.CommandArguments())
		case "export":
			handleExportCommand(bot, chatID, message.CommandArguments())
		case "analiz":
			handleAnalizCommand(bot, chatID, message.CommandArguments())
		case "kalem":
			handleKalemCommand(bot, chatID, message.CommandArguments())
		case "google":
			handleSourceAnalysisCommand(bot, chatID, "google")
		case "meta":
			handleSourceAnalysisCommand(bot, chatID, "meta")
		default:
			msg := tgbotapi.NewMessage(chatID, "Bilinmeyen komut. /start komutu ile kullanılabilir komutları görebilirsiniz.")
			bot.Send(msg)
		}
		return
	}

	// Aktif session varsa, kullanıcı girdisini işle (session yoksa cevap verme)
	sessionsMutex.RLock()
	session, exists := sessions[userID]
	sessionsMutex.RUnlock()

	if exists {
		handleUserInput(bot, chatID, userID, message.Text, session)
	}
}

// sendMyID kullanıcıya chat ID'sini gösterir
func sendMyID(bot *tgbotapi.BotAPI, chatID int64, userID int64) {
	text := fmt.Sprintf(`🆔 *Chat ve Kullanıcı Bilgileriniz*

*Chat ID:* `+"`%d`"+`
*User ID:* `+"`%d`"+`

Bu Chat ID'yi NOTIFICATION_CHAT_ID olarak kullanabilirsiniz.`, chatID, userID)

	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = "Markdown"
	bot.Send(msg)
}

// handleToplamCommand /toplam komutunu işler
func handleToplamCommand(bot *tgbotapi.BotAPI, chatID int64, args string) {
	ctx := context.Background()
	args = strings.TrimSpace(args)

	var startDate, endDate time.Time
	var hasDateFilter bool

	// Tarih aralığı parse et (DD.MM.YYYY - DD.MM.YYYY formatı)
	if args != "" {
		parts := strings.Split(args, "-")
		if len(parts) == 2 {
			startStr := strings.TrimSpace(parts[0])
			endStr := strings.TrimSpace(parts[1])

			var err error
			startDate, err = time.Parse("02.01.2006", startStr)
			if err != nil {
				msg := tgbotapi.NewMessage(chatID, "⚠️ Geçersiz tarih formatı.\n\nKullanım:\n/toplam - Tüm bağışlar\n/toplam DD.MM.YYYY - DD.MM.YYYY - Tarih aralığı")
				bot.Send(msg)
				return
			}

			endDate, err = time.Parse("02.01.2006", endStr)
			if err != nil {
				msg := tgbotapi.NewMessage(chatID, "⚠️ Geçersiz tarih formatı.\n\nKullanım:\n/toplam - Tüm bağışlar\n/toplam DD.MM.YYYY - DD.MM.YYYY - Tarih aralığı")
				bot.Send(msg)
				return
			}

			// Bitiş tarihini günün sonuna ayarla (23:59:59)
			endDate = endDate.Add(23*time.Hour + 59*time.Minute + 59*time.Second)
			hasDateFilter = true
		} else {
			msg := tgbotapi.NewMessage(chatID, "⚠️ Geçersiz format.\n\nKullanım:\n/toplam - Tüm bağışlar\n/toplam DD.MM.YYYY - DD.MM.YYYY - Tarih aralığı")
			bot.Send(msg)
			return
		}
	}

	// Sorguları hazırla
	var totalAmount float64
	var orderCount int
	var currencyTotals []struct {
		Currency string  `bun:"currency"`
		Total    float64 `bun:"total"`
		Count    int     `bun:"count"`
	}

	// Para birimi bazında toplam
	query := db.NewSelect().
		TableExpr("orders").
		ColumnExpr("currency").
		ColumnExpr("SUM(amount) as total").
		ColumnExpr("COUNT(*) as count").
		GroupExpr("currency")

	if hasDateFilter {
		query = query.Where("event_time >= ?", startDate).Where("event_time <= ?", endDate)
	}

	err := query.Scan(ctx, &currencyTotals)
	if err != nil {
		log.Printf("Toplam sorgu hatası: %v", err)
		msg := tgbotapi.NewMessage(chatID, "❌ Veritabanı sorgu hatası oluştu.")
		bot.Send(msg)
		return
	}

	// Toplam hesapla
	for _, ct := range currencyTotals {
		totalAmount += ct.Total
		orderCount += ct.Count
	}

	// Mesajı oluştur
	var sb strings.Builder
	sb.WriteString("📊 <b>Bağış Özeti</b>\n\n")

	if hasDateFilter {
		sb.WriteString(fmt.Sprintf("📅 <b>Tarih Aralığı:</b> %s - %s\n\n",
			startDate.Format("02.01.2006"),
			endDate.Format("02.01.2006")))
	} else {
		sb.WriteString("📅 <b>Dönem:</b> Tüm zamanlar\n\n")
	}

	if orderCount == 0 {
		sb.WriteString("ℹ️ Bu dönemde bağış bulunmamaktadır.")
	} else {
		sb.WriteString(fmt.Sprintf("🛒 <b>Toplam Bağış Sayısı:</b> %d\n\n", orderCount))

		sb.WriteString("💰 <b>Para Birimi Bazında:</b>\n")
		for _, ct := range currencyTotals {
			sb.WriteString(fmt.Sprintf("  • %s: %.2f (%d bağış)\n", ct.Currency, ct.Total, ct.Count))
		}
	}

	msg := tgbotapi.NewMessage(chatID, sb.String())
	msg.ParseMode = "HTML"
	bot.Send(msg)
}

// handleKaynaklarCommand /kaynaklar komutunu işler - UTM source bazlı analiz
func handleKaynaklarCommand(bot *tgbotapi.BotAPI, chatID int64, args string) {
	ctx := context.Background()
	startDate, endDate, hasDateFilter := parseDateRange(args)

	var sources []struct {
		UTMSource string  `bun:"utm_source"`
		Total     float64 `bun:"total"`
		Count     int     `bun:"count"`
	}

	query := db.NewSelect().
		TableExpr("orders").
		ColumnExpr("COALESCE(utm_source, 'Bilinmiyor') as utm_source").
		ColumnExpr("SUM(amount) as total").
		ColumnExpr("COUNT(*) as count").
		GroupExpr("utm_source").
		OrderExpr("total DESC")

	if hasDateFilter {
		query = query.Where("event_time >= ?", startDate).Where("event_time <= ?", endDate)
	}

	err := query.Scan(ctx, &sources)
	if err != nil {
		log.Printf("Kaynaklar sorgu hatası: %v", err)
		msg := tgbotapi.NewMessage(chatID, "❌ Veritabanı sorgu hatası oluştu.")
		bot.Send(msg)
		return
	}

	// Toplam hesapla
	var grandTotal float64
	for _, s := range sources {
		grandTotal += s.Total
	}

	var sb strings.Builder
	sb.WriteString("📊 <b>Kaynak Bazlı Analiz (UTM Source)</b>\n\n")

	if hasDateFilter {
		sb.WriteString(fmt.Sprintf("📅 <b>Tarih:</b> %s - %s\n\n", startDate.Format("02.01.2006"), endDate.Format("02.01.2006")))
	}

	if len(sources) == 0 {
		sb.WriteString("ℹ️ Bu dönemde veri bulunmamaktadır.")
	} else {
		for i, s := range sources {
			percentage := (s.Total / grandTotal) * 100
			emoji := getEmojiByRank(i)
			sb.WriteString(fmt.Sprintf("%s <b>%s</b>\n", emoji, s.UTMSource))
			sb.WriteString(fmt.Sprintf("   💰 %.2f TRY (%d bağış) - %%%.1f\n\n", s.Total, s.Count, percentage))
		}
		sb.WriteString(fmt.Sprintf("📈 <b>Toplam:</b> %.2f TRY", grandTotal))
	}

	msg := tgbotapi.NewMessage(chatID, sb.String())
	msg.ParseMode = "HTML"
	bot.Send(msg)
}

// handleKampanyalarCommand /kampanyalar komutunu işler - Kampanya performansı
func handleKampanyalarCommand(bot *tgbotapi.BotAPI, chatID int64, args string) {
	ctx := context.Background()
	startDate, endDate, hasDateFilter := parseDateRange(args)

	var campaigns []struct {
		UTMCampaign string  `bun:"utm_campaign"`
		Total       float64 `bun:"total"`
		Count       int     `bun:"count"`
		AvgAmount   float64 `bun:"avg_amount"`
	}

	query := db.NewSelect().
		TableExpr("orders").
		ColumnExpr("COALESCE(utm_campaign, 'Bilinmiyor') as utm_campaign").
		ColumnExpr("SUM(amount) as total").
		ColumnExpr("COUNT(*) as count").
		ColumnExpr("AVG(amount) as avg_amount").
		GroupExpr("utm_campaign").
		OrderExpr("total DESC").
		Limit(10)

	if hasDateFilter {
		query = query.Where("event_time >= ?", startDate).Where("event_time <= ?", endDate)
	}

	err := query.Scan(ctx, &campaigns)
	if err != nil {
		log.Printf("Kampanyalar sorgu hatası: %v", err)
		msg := tgbotapi.NewMessage(chatID, "❌ Veritabanı sorgu hatası oluştu.")
		bot.Send(msg)
		return
	}

	var sb strings.Builder
	sb.WriteString("🎯 <b>Kampanya Performansı (Top 10)</b>\n\n")

	if hasDateFilter {
		sb.WriteString(fmt.Sprintf("📅 <b>Tarih:</b> %s - %s\n\n", startDate.Format("02.01.2006"), endDate.Format("02.01.2006")))
	}

	if len(campaigns) == 0 {
		sb.WriteString("ℹ️ Bu dönemde kampanya verisi bulunmamaktadır.")
	} else {
		for i, c := range campaigns {
			emoji := getEmojiByRank(i)
			sb.WriteString(fmt.Sprintf("%s <b>%s</b>\n", emoji, c.UTMCampaign))
			sb.WriteString(fmt.Sprintf("   💰 %.2f TRY | 🛒 %d bağış | 📊 Ort: %.2f TRY\n\n", c.Total, c.Count, c.AvgAmount))
		}
	}

	msg := tgbotapi.NewMessage(chatID, sb.String())
	msg.ParseMode = "HTML"
	bot.Send(msg)
}

// handleOrtamlarCommand /ortamlar komutunu işler - UTM medium bazlı analiz
func handleOrtamlarCommand(bot *tgbotapi.BotAPI, chatID int64, args string) {
	ctx := context.Background()
	startDate, endDate, hasDateFilter := parseDateRange(args)

	var mediums []struct {
		UTMMedium string  `bun:"utm_medium"`
		Total     float64 `bun:"total"`
		Count     int     `bun:"count"`
	}

	query := db.NewSelect().
		TableExpr("orders").
		ColumnExpr("COALESCE(utm_medium, 'Bilinmiyor') as utm_medium").
		ColumnExpr("SUM(amount) as total").
		ColumnExpr("COUNT(*) as count").
		GroupExpr("utm_medium").
		OrderExpr("total DESC")

	if hasDateFilter {
		query = query.Where("event_time >= ?", startDate).Where("event_time <= ?", endDate)
	}

	err := query.Scan(ctx, &mediums)
	if err != nil {
		log.Printf("Ortamlar sorgu hatası: %v", err)
		msg := tgbotapi.NewMessage(chatID, "❌ Veritabanı sorgu hatası oluştu.")
		bot.Send(msg)
		return
	}

	var grandTotal float64
	for _, m := range mediums {
		grandTotal += m.Total
	}

	var sb strings.Builder
	sb.WriteString("📡 <b>Reklam Ortamı Analizi (UTM Medium)</b>\n\n")

	if hasDateFilter {
		sb.WriteString(fmt.Sprintf("📅 <b>Tarih:</b> %s - %s\n\n", startDate.Format("02.01.2006"), endDate.Format("02.01.2006")))
	}

	if len(mediums) == 0 {
		sb.WriteString("ℹ️ Bu dönemde veri bulunmamaktadır.")
	} else {
		for _, m := range mediums {
			percentage := (m.Total / grandTotal) * 100
			emoji := getMediumEmoji(m.UTMMedium)
			sb.WriteString(fmt.Sprintf("%s <b>%s</b>\n", emoji, m.UTMMedium))
			sb.WriteString(fmt.Sprintf("   💰 %.2f TRY (%d bağış) - %%%.1f\n\n", m.Total, m.Count, percentage))
		}
		sb.WriteString(fmt.Sprintf("📈 <b>Toplam:</b> %.2f TRY", grandTotal))
	}

	msg := tgbotapi.NewMessage(chatID, sb.String())
	msg.ParseMode = "HTML"
	bot.Send(msg)
}

// handleSonCommand /son komutunu işler - Son N bağış
func handleSonCommand(bot *tgbotapi.BotAPI, chatID int64, args string) {
	ctx := context.Background()

	// Varsayılan 5, argüman varsa onu kullan
	limit := 5
	if args != "" {
		if n, err := strconv.Atoi(strings.TrimSpace(args)); err == nil && n > 0 && n <= 20 {
			limit = n
		}
	}

	var orders []Order
	err := db.NewSelect().
		Model(&orders).
		OrderExpr("event_time DESC").
		Limit(limit).
		Scan(ctx)

	if err != nil {
		log.Printf("Son bağışlar sorgu hatası: %v", err)
		msg := tgbotapi.NewMessage(chatID, "❌ Veritabanı sorgu hatası oluştu.")
		bot.Send(msg)
		return
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("🕐 <b>Son %d Bağış</b>\n\n", limit))

	if len(orders) == 0 {
		sb.WriteString("ℹ️ Henüz bağış bulunmamaktadır.")
	} else {
		for i, o := range orders {
			sb.WriteString(fmt.Sprintf("<b>%d.</b> 💰 %.2f %s\n", i+1, o.Amount, o.Currency))
			sb.WriteString(fmt.Sprintf("   📅 %s\n", o.EventTime.Format("02.01.2006 15:04")))
			if o.UTMSource != "" {
				sb.WriteString(fmt.Sprintf("   📊 %s / %s\n", o.UTMSource, o.UTMMedium))
			}
			if o.UTMCampaign != "" {
				sb.WriteString(fmt.Sprintf("   🎯 %s\n", o.UTMCampaign))
			}
			if o.GadSource != "" || o.GadCampaignID != "" {
				sb.WriteString(fmt.Sprintf("   🔍 Google: %s / %s\n", o.GadSource, o.GadCampaignID))
			}
			if o.TrafficChannel != "" {
				sb.WriteString(fmt.Sprintf("   📡 Kanal: %s\n", o.TrafficChannel))
			}
			sb.WriteString("\n")
		}
	}

	msg := tgbotapi.NewMessage(chatID, sb.String())
	msg.ParseMode = "HTML"
	bot.Send(msg)
}

// handleGunlukCommand /gunluk komutunu işler - Bugünün özeti
func handleGunlukCommand(bot *tgbotapi.BotAPI, chatID int64) {
	ctx := context.Background()

	// Türkiye saati için UTC+3 ekle (timezone dosyası olmayabilir)
	now := time.Now().UTC().Add(3 * time.Hour)
	startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	endOfDay := startOfDay.Add(24 * time.Hour)

	// UTC olarak kullan (zaten UTC'de hesapladık)
	startOfDayUTC := startOfDay.Add(-3 * time.Hour)
	endOfDayUTC := endOfDay.Add(-3 * time.Hour)

	// Genel istatistikler
	var stats struct {
		Total float64 `bun:"total"`
		Count int     `bun:"count"`
	}
	err := db.NewSelect().
		TableExpr("orders").
		ColumnExpr("COALESCE(SUM(amount), 0) as total").
		ColumnExpr("COUNT(*) as count").
		Where("event_time >= ?", startOfDayUTC).
		Where("event_time < ?", endOfDayUTC).
		Scan(ctx, &stats)

	if err != nil {
		log.Printf("Günlük sorgu hatası: %v", err)
		msg := tgbotapi.NewMessage(chatID, "❌ Veritabanı sorgu hatası oluştu.")
		bot.Send(msg)
		return
	}

	// Kaynak bazlı dağılım (traffic_channel ile birlikte)
	var sources []struct {
		UTMSource string  `bun:"utm_source"`
		Total     float64 `bun:"total"`
		Count     int     `bun:"count"`
	}
	db.NewRaw(`
		SELECT 
			CASE 
				WHEN utm_source IS NOT NULL AND utm_source != '' THEN utm_source
				WHEN traffic_channel = 'google' THEN 'Google Ads'
				ELSE 'Doğrudan'
			END as utm_source,
			SUM(amount) as total,
			COUNT(*) as count
		FROM orders
		WHERE event_time >= ? AND event_time < ?
		GROUP BY 1
		ORDER BY total DESC
	`, startOfDayUTC, endOfDayUTC).Scan(ctx, &sources)

	// Türkçe gün adı
	gunAdi := getTurkishDayName(now.Weekday())

	var sb strings.Builder
	sb.WriteString("━━━━━━━━━━━━━━━━━━━━━━\n")
	sb.WriteString("☀️ <b>GÜNLÜK RAPOR</b>\n")
	sb.WriteString("━━━━━━━━━━━━━━━━━━━━━━\n\n")
	sb.WriteString(fmt.Sprintf("📅 <b>Tarih:</b> %s, %s\n", now.Format("02 Ocak 2006"), gunAdi))
	sb.WriteString(fmt.Sprintf("🕐 <b>Saat:</b> %s\n\n", now.Format("15:04")))

	if stats.Count == 0 {
		sb.WriteString("━━━━━━━━━━━━━━━━━━━━━━\n")
		sb.WriteString("ℹ️ Bugün henüz bağış bulunmamaktadır.\n")
		sb.WriteString("━━━━━━━━━━━━━━━━━━━━━━\n")
	} else {
		sb.WriteString("━━━━━━━━━━━━━━━━━━━━━━\n")
		sb.WriteString("💰 <b>GENEL ÖZET</b>\n")
		sb.WriteString("━━━━━━━━━━━━━━━━━━━━━━\n\n")
		sb.WriteString(fmt.Sprintf("   🛒 Bağış Sayısı    : <b>%d</b>\n", stats.Count))
		sb.WriteString(fmt.Sprintf("   💵 Toplam Tutar    : <b>%.2f TRY</b>\n", stats.Total))
		sb.WriteString(fmt.Sprintf("   📊 Ortalama        : <b>%.2f TRY</b>\n\n", stats.Total/float64(stats.Count)))

		if len(sources) > 0 {
			sb.WriteString("━━━━━━━━━━━━━━━━━━━━━━\n")
			sb.WriteString("📡 <b>KAYNAK DAĞILIMI</b>\n")
			sb.WriteString("━━━━━━━━━━━━━━━━━━━━━━\n\n")

			for i, s := range sources {
				emoji := getEmojiByRank(i)
				percentage := (s.Total / stats.Total) * 100
				sb.WriteString(fmt.Sprintf("%s <b>%s</b>\n", emoji, s.UTMSource))
				sb.WriteString(fmt.Sprintf("   └ %.2f TRY | %d bağış | %%%.1f\n\n", s.Total, s.Count, percentage))
			}
		}
		sb.WriteString("━━━━━━━━━━━━━━━━━━━━━━\n")
	}

	msg := tgbotapi.NewMessage(chatID, sb.String())
	msg.ParseMode = "HTML"
	bot.Send(msg)
}

// getTurkishDayName gün numarasını Türkçe gün adına çevirir
func getTurkishDayName(day time.Weekday) string {
	days := map[time.Weekday]string{
		time.Sunday:    "Pazar",
		time.Monday:    "Pazartesi",
		time.Tuesday:   "Salı",
		time.Wednesday: "Çarşamba",
		time.Thursday:  "Perşembe",
		time.Friday:    "Cuma",
		time.Saturday:  "Cumartesi",
	}
	return days[day]
}

// handleOrtalamaCommand /ortalama komutunu işler - Ortalama bağış analizi
func handleOrtalamaCommand(bot *tgbotapi.BotAPI, chatID int64, args string) {
	ctx := context.Background()
	startDate, endDate, hasDateFilter := parseDateRange(args)

	// Kaynak bazlı ortalama
	var sourceAvg []struct {
		UTMSource string  `bun:"utm_source"`
		AvgAmount float64 `bun:"avg_amount"`
		Count     int     `bun:"count"`
		Total     float64 `bun:"total"`
	}

	query := db.NewSelect().
		TableExpr("orders").
		ColumnExpr("COALESCE(utm_source, 'Bilinmiyor') as utm_source").
		ColumnExpr("AVG(amount) as avg_amount").
		ColumnExpr("COUNT(*) as count").
		ColumnExpr("SUM(amount) as total").
		GroupExpr("utm_source").
		OrderExpr("avg_amount DESC")

	if hasDateFilter {
		query = query.Where("event_time >= ?", startDate).Where("event_time <= ?", endDate)
	}

	err := query.Scan(ctx, &sourceAvg)
	if err != nil {
		log.Printf("Ortalama sorgu hatası: %v", err)
		msg := tgbotapi.NewMessage(chatID, "❌ Veritabanı sorgu hatası oluştu.")
		bot.Send(msg)
		return
	}

	// Kampanya bazlı ortalama (top 5)
	var campaignAvg []struct {
		UTMCampaign string  `bun:"utm_campaign"`
		AvgAmount   float64 `bun:"avg_amount"`
		Count       int     `bun:"count"`
	}

	query2 := db.NewSelect().
		TableExpr("orders").
		ColumnExpr("COALESCE(utm_campaign, 'Bilinmiyor') as utm_campaign").
		ColumnExpr("AVG(amount) as avg_amount").
		ColumnExpr("COUNT(*) as count").
		GroupExpr("utm_campaign").
		OrderExpr("avg_amount DESC").
		Limit(5)

	if hasDateFilter {
		query2 = query2.Where("event_time >= ?", startDate).Where("event_time <= ?", endDate)
	}

	query2.Scan(ctx, &campaignAvg)

	var sb strings.Builder
	sb.WriteString("📊 <b>Ortalama Bağış Analizi</b>\n\n")

	if hasDateFilter {
		sb.WriteString(fmt.Sprintf("📅 <b>Tarih:</b> %s - %s\n\n", startDate.Format("02.01.2006"), endDate.Format("02.01.2006")))
	}

	if len(sourceAvg) == 0 {
		sb.WriteString("ℹ️ Bu dönemde veri bulunmamaktadır.")
	} else {
		sb.WriteString("<b>🎯 Kaynak Bazlı Ortalama:</b>\n")
		sb.WriteString("<i>(Hangi kaynak daha kaliteli bağışçı getiriyor?)</i>\n\n")
		for _, s := range sourceAvg {
			sb.WriteString(fmt.Sprintf("• <b>%s</b>\n", s.UTMSource))
			sb.WriteString(fmt.Sprintf("  Ort: %.2f TRY | %d bağış | Toplam: %.2f TRY\n\n", s.AvgAmount, s.Count, s.Total))
		}

		if len(campaignAvg) > 0 {
			sb.WriteString("\n<b>🏆 En Yüksek Ortalama Kampanyalar (Top 5):</b>\n\n")
			for i, c := range campaignAvg {
				emoji := getEmojiByRank(i)
				sb.WriteString(fmt.Sprintf("%s <b>%s</b>\n", emoji, c.UTMCampaign))
				sb.WriteString(fmt.Sprintf("   Ort: %.2f TRY (%d bağış)\n\n", c.AvgAmount, c.Count))
			}
		}
	}

	msg := tgbotapi.NewMessage(chatID, sb.String())
	msg.ParseMode = "HTML"
	bot.Send(msg)
}

// handleExportCommand /export komutunu işler - Excel export
func handleExportCommand(bot *tgbotapi.BotAPI, chatID int64, args string) {
	ctx := context.Background()
	startDate, endDate, hasDateFilter := parseDateRange(args)

	var orders []Order
	query := db.NewSelect().Model(&orders).OrderExpr("event_time DESC")

	if hasDateFilter {
		query = query.Where("event_time >= ?", startDate).Where("event_time <= ?", endDate)
	}

	err := query.Scan(ctx)
	if err != nil {
		log.Printf("Export sorgu hatası: %v", err)
		msg := tgbotapi.NewMessage(chatID, "❌ Veritabanı sorgu hatası oluştu.")
		bot.Send(msg)
		return
	}

	if len(orders) == 0 {
		msg := tgbotapi.NewMessage(chatID, "ℹ️ Dışa aktarılacak veri bulunmamaktadır.")
		bot.Send(msg)
		return
	}

	// Excel dosyası oluştur
	f := excelize.NewFile()
	defer f.Close()

	sheetName := "Bağışlar"
	f.SetSheetName("Sheet1", sheetName)

	// Başlık stilleri
	headerStyle, _ := f.NewStyle(&excelize.Style{
		Font:      &excelize.Font{Bold: true, Color: "FFFFFF", Size: 11},
		Fill:      excelize.Fill{Type: "pattern", Color: []string{"4472C4"}, Pattern: 1},
		Alignment: &excelize.Alignment{Horizontal: "center", Vertical: "center"},
		Border: []excelize.Border{
			{Type: "left", Color: "000000", Style: 1},
			{Type: "top", Color: "000000", Style: 1},
			{Type: "bottom", Color: "000000", Style: 1},
			{Type: "right", Color: "000000", Style: 1},
		},
	})

	// Başlıklar
	headers := []string{"Sipariş ID", "Tutar", "Para Birimi", "Bağış Kalemleri", "UTM Source", "UTM Medium", "UTM Campaign", "UTM Content", "UTM Term", "GAD Source", "GAD Campaign ID", "Traffic Channel", "Tarih", "Kayıt Tarihi"}
	for i, h := range headers {
		cell, _ := excelize.CoordinatesToCellName(i+1, 1)
		f.SetCellValue(sheetName, cell, h)
		f.SetCellStyle(sheetName, cell, cell, headerStyle)
	}

	// Veri stilleri
	dataStyle, _ := f.NewStyle(&excelize.Style{
		Border: []excelize.Border{
			{Type: "left", Color: "000000", Style: 1},
			{Type: "top", Color: "000000", Style: 1},
			{Type: "bottom", Color: "000000", Style: 1},
			{Type: "right", Color: "000000", Style: 1},
		},
		Alignment: &excelize.Alignment{Vertical: "center"},
	})

	amountStyle, _ := f.NewStyle(&excelize.Style{
		NumFmt: 4, // #,##0.00
		Border: []excelize.Border{
			{Type: "left", Color: "000000", Style: 1},
			{Type: "top", Color: "000000", Style: 1},
			{Type: "bottom", Color: "000000", Style: 1},
			{Type: "right", Color: "000000", Style: 1},
		},
		Alignment: &excelize.Alignment{Horizontal: "right", Vertical: "center"},
	})

	// Verileri ekle
	for i, o := range orders {
		row := i + 2

		// Bağış kalemlerini string'e çevir
		var itemsStr string
		for j, item := range o.Items {
			if j > 0 {
				itemsStr += ", "
			}
			itemsStr += fmt.Sprintf("%s (x%d)", item.ItemName, item.Quantity)
		}

		f.SetCellValue(sheetName, fmt.Sprintf("A%d", row), o.OrderID)
		f.SetCellValue(sheetName, fmt.Sprintf("B%d", row), o.Amount)
		f.SetCellValue(sheetName, fmt.Sprintf("C%d", row), o.Currency)
		f.SetCellValue(sheetName, fmt.Sprintf("D%d", row), itemsStr)
		f.SetCellValue(sheetName, fmt.Sprintf("E%d", row), o.UTMSource)
		f.SetCellValue(sheetName, fmt.Sprintf("F%d", row), o.UTMMedium)
		f.SetCellValue(sheetName, fmt.Sprintf("G%d", row), o.UTMCampaign)
		f.SetCellValue(sheetName, fmt.Sprintf("H%d", row), o.UTMContent)
		f.SetCellValue(sheetName, fmt.Sprintf("I%d", row), o.UTMTerm)
		f.SetCellValue(sheetName, fmt.Sprintf("J%d", row), o.GadSource)
		f.SetCellValue(sheetName, fmt.Sprintf("K%d", row), o.GadCampaignID)
		f.SetCellValue(sheetName, fmt.Sprintf("L%d", row), o.TrafficChannel)
		f.SetCellValue(sheetName, fmt.Sprintf("M%d", row), o.EventTime.Format("02.01.2006 15:04:05"))
		f.SetCellValue(sheetName, fmt.Sprintf("N%d", row), o.CreatedAt.Format("02.01.2006 15:04:05"))

		// Stiller uygula
		for col := 1; col <= 14; col++ {
			cell, _ := excelize.CoordinatesToCellName(col, row)
			if col == 2 {
				f.SetCellStyle(sheetName, cell, cell, amountStyle)
			} else {
				f.SetCellStyle(sheetName, cell, cell, dataStyle)
			}
		}
	}

	// Sütun genişlikleri
	f.SetColWidth(sheetName, "A", "A", 40)
	f.SetColWidth(sheetName, "B", "B", 12)
	f.SetColWidth(sheetName, "C", "C", 10)
	f.SetColWidth(sheetName, "D", "D", 40)
	f.SetColWidth(sheetName, "E", "E", 12)
	f.SetColWidth(sheetName, "F", "F", 15)
	f.SetColWidth(sheetName, "G", "G", 25)
	f.SetColWidth(sheetName, "H", "H", 20)
	f.SetColWidth(sheetName, "I", "I", 15)
	f.SetColWidth(sheetName, "J", "J", 12)
	f.SetColWidth(sheetName, "K", "K", 18)
	f.SetColWidth(sheetName, "L", "L", 15)
	f.SetColWidth(sheetName, "M", "M", 18)
	f.SetColWidth(sheetName, "N", "N", 18)

	// Özet sayfası ekle
	summarySheet := "Özet"
	f.NewSheet(summarySheet)

	// Özet başlığı
	f.SetCellValue(summarySheet, "A1", "📊 Bağış Raporu Özeti")
	f.MergeCell(summarySheet, "A1", "C1")
	titleStyle, _ := f.NewStyle(&excelize.Style{
		Font:      &excelize.Font{Bold: true, Size: 14, Color: "4472C4"},
		Alignment: &excelize.Alignment{Horizontal: "center"},
	})
	f.SetCellStyle(summarySheet, "A1", "C1", titleStyle)

	// Tarih aralığı
	if hasDateFilter {
		f.SetCellValue(summarySheet, "A3", fmt.Sprintf("Tarih Aralığı: %s - %s", startDate.Format("02.01.2006"), endDate.Format("02.01.2006")))
	} else {
		f.SetCellValue(summarySheet, "A3", "Dönem: Tüm Zamanlar")
	}

	// Genel istatistikler
	var totalAmount float64
	for _, o := range orders {
		totalAmount += o.Amount
	}
	avgAmount := totalAmount / float64(len(orders))

	f.SetCellValue(summarySheet, "A5", "Toplam Bağış Sayısı:")
	f.SetCellValue(summarySheet, "B5", len(orders))
	f.SetCellValue(summarySheet, "A6", "Toplam Tutar:")
	f.SetCellValue(summarySheet, "B6", fmt.Sprintf("%.2f TRY", totalAmount))
	f.SetCellValue(summarySheet, "A7", "Ortalama Bağış:")
	f.SetCellValue(summarySheet, "B7", fmt.Sprintf("%.2f TRY", avgAmount))

	f.SetColWidth(summarySheet, "A", "A", 25)
	f.SetColWidth(summarySheet, "B", "B", 20)

	// Dosyayı kaydet
	var filename string
	if hasDateFilter {
		filename = fmt.Sprintf("bagislar_%s_%s.xlsx", startDate.Format("02-01-2006"), endDate.Format("02-01-2006"))
	} else {
		filename = fmt.Sprintf("bagislar_tum_%s.xlsx", time.Now().Format("02-01-2006"))
	}

	filepath := fmt.Sprintf("/tmp/%s", filename)
	if err := f.SaveAs(filepath); err != nil {
		log.Printf("Excel kayıt hatası: %v", err)
		msg := tgbotapi.NewMessage(chatID, "❌ Excel dosyası oluşturulamadı.")
		bot.Send(msg)
		return
	}

	// Telegram'a gönder
	doc := tgbotapi.NewDocument(chatID, tgbotapi.FilePath(filepath))
	doc.Caption = fmt.Sprintf("📊 Bağış Raporu\n📁 %d kayıt\n💰 Toplam: %.2f TRY", len(orders), totalAmount)

	if _, err := bot.Send(doc); err != nil {
		log.Printf("Dosya gönderme hatası: %v", err)
		msg := tgbotapi.NewMessage(chatID, "❌ Dosya gönderilemedi.")
		bot.Send(msg)
		return
	}

	// Geçici dosyayı sil
	os.Remove(filepath)
}

// handleAnalizCommand /analiz komutunu işler - UTM linkinden bağış analizi
func handleAnalizCommand(bot *tgbotapi.BotAPI, chatID int64, args string) {
	args = strings.TrimSpace(args)

	if args == "" {
		msg := tgbotapi.NewMessage(chatID, `📊 <b>Link Analizi</b>

UTM parametreli bir link gönderin, o linke ait tüm bağışları listeleyelim.

<b>Kullanım:</b>
<code>/analiz https://hayratyardim.org/bagis/su-kuyusu/?utm_source=google&amp;utm_campaign=test</code>

Link içindeki UTM parametreleri (utm_source, utm_medium, utm_campaign) kullanılarak eşleşen bağışlar bulunur.`)
		msg.ParseMode = "HTML"
		bot.Send(msg)
		return
	}

	// URL'yi parse et
	parsedURL, err := url.Parse(args)
	if err != nil {
		msg := tgbotapi.NewMessage(chatID, "❌ Geçersiz URL formatı.")
		bot.Send(msg)
		return
	}

	// UTM parametrelerini çıkar
	query := parsedURL.Query()
	utmSource := query.Get("utm_source")
	utmMedium := query.Get("utm_medium")
	utmCampaign := query.Get("utm_campaign")

	if utmSource == "" && utmMedium == "" && utmCampaign == "" {
		msg := tgbotapi.NewMessage(chatID, "⚠️ Bu linkte UTM parametresi bulunamadı.\n\nÖrnek: ?utm_source=google&utm_campaign=test")
		bot.Send(msg)
		return
	}

	ctx := context.Background()

	// Sorguyu oluştur
	var orders []Order
	queryBuilder := db.NewSelect().Model(&orders)

	// Filtreleri ekle (sadece dolu olanlar)
	if utmSource != "" {
		queryBuilder = queryBuilder.Where("utm_source = ?", utmSource)
	}
	if utmMedium != "" {
		queryBuilder = queryBuilder.Where("utm_medium = ?", utmMedium)
	}
	if utmCampaign != "" {
		queryBuilder = queryBuilder.Where("utm_campaign = ?", utmCampaign)
	}

	queryBuilder = queryBuilder.OrderExpr("event_time DESC").Limit(50)

	err = queryBuilder.Scan(ctx)
	if err != nil {
		log.Printf("Analiz sorgu hatası: %v", err)
		msg := tgbotapi.NewMessage(chatID, "❌ Veritabanı sorgu hatası oluştu.")
		bot.Send(msg)
		return
	}

	// İstatistikleri hesapla
	var totalAmount float64
	for _, o := range orders {
		totalAmount += o.Amount
	}

	// Mesajı oluştur
	var sb strings.Builder
	sb.WriteString("🔍 <b>Link Analizi Sonuçları</b>\n\n")

	sb.WriteString("<b>🎯 Arama Kriterleri:</b>\n")
	if utmSource != "" {
		sb.WriteString(fmt.Sprintf("  • utm_source: <code>%s</code>\n", utmSource))
	}
	if utmMedium != "" {
		sb.WriteString(fmt.Sprintf("  • utm_medium: <code>%s</code>\n", utmMedium))
	}
	if utmCampaign != "" {
		sb.WriteString(fmt.Sprintf("  • utm_campaign: <code>%s</code>\n", utmCampaign))
	}
	sb.WriteString("\n")

	if len(orders) == 0 {
		sb.WriteString("ℹ️ Bu kriterlere uyan bağış bulunamadı.")
	} else {
		sb.WriteString(fmt.Sprintf("📈 <b>Özet:</b>\n"))
		sb.WriteString(fmt.Sprintf("  • Toplam Bağış: %d\n", len(orders)))
		sb.WriteString(fmt.Sprintf("  • Toplam Tutar: %.2f TRY\n", totalAmount))
		if len(orders) > 0 {
			sb.WriteString(fmt.Sprintf("  • Ortalama: %.2f TRY\n", totalAmount/float64(len(orders))))
		}
		sb.WriteString("\n")

		// Son 10 bağışı listele
		limit := 10
		if len(orders) < limit {
			limit = len(orders)
		}
		sb.WriteString(fmt.Sprintf("🕐 <b>Son %d Bağış:</b>\n", limit))
		for i := 0; i < limit; i++ {
			o := orders[i]
			sb.WriteString(fmt.Sprintf("%d. %.2f %s - %s\n", i+1, o.Amount, o.Currency, o.EventTime.Format("02.01.2006 15:04")))
		}

		if len(orders) > 10 {
			sb.WriteString(fmt.Sprintf("\n<i>...ve %d bağış daha</i>", len(orders)-10))
		}
	}

	msg := tgbotapi.NewMessage(chatID, sb.String())
	msg.ParseMode = "HTML"
	bot.Send(msg)
}

// parseDateRange tarih aralığını parse eder
func parseDateRange(args string) (startDate, endDate time.Time, hasFilter bool) {
	args = strings.TrimSpace(args)
	if args == "" {
		return time.Time{}, time.Time{}, false
	}

	parts := strings.Split(args, "-")
	if len(parts) != 2 {
		return time.Time{}, time.Time{}, false
	}

	startStr := strings.TrimSpace(parts[0])
	endStr := strings.TrimSpace(parts[1])

	var err error
	startDate, err = time.Parse("02.01.2006", startStr)
	if err != nil {
		return time.Time{}, time.Time{}, false
	}

	endDate, err = time.Parse("02.01.2006", endStr)
	if err != nil {
		return time.Time{}, time.Time{}, false
	}

	// Bitiş tarihini günün sonuna ayarla
	endDate = endDate.Add(23*time.Hour + 59*time.Minute + 59*time.Second)
	return startDate, endDate, true
}

// getEmojiByRank sıraya göre emoji döner
func getEmojiByRank(rank int) string {
	switch rank {
	case 0:
		return "🥇"
	case 1:
		return "🥈"
	case 2:
		return "🥉"
	default:
		return "▫️"
	}
}

// getMediumEmoji medium tipine göre emoji döner
func getMediumEmoji(medium string) string {
	switch strings.ToLower(medium) {
	case "paid_social":
		return "📱"
	case "cpc":
		return "🔍"
	case "display":
		return "🖼️"
	case "organic_social":
		return "🌿"
	case "email":
		return "📧"
	case "sms":
		return "💬"
	default:
		return "📊"
	}
}

// sendWelcomeMessage hoş geldin mesajı gönderir
func sendWelcomeMessage(bot *tgbotapi.BotAPI, chatID int64) {
	welcomeText := `🔗 <b>Hayrat Yardım UTM Builder Bot'a Hoş Geldiniz!</b>

Bu bot, pazarlama kampanyalarınız için UTM parametreli linkler oluşturmanıza ve reklam performansını analiz etmenize yardımcı olur.

<b>📊 Analiz Komutları:</b>
/toplam - Tüm bağışların özeti
/toplam DD.MM.YYYY - DD.MM.YYYY - Tarih aralığı
/kaynaklar - Kaynak bazlı analiz (meta, google vb.)
/kampanyalar - Kampanya performansı
/ortamlar - Reklam ortamı analizi
/gunluk - Bugünün özeti
/son [N] - Son N bağış (varsayılan 5)
/ortalama - Ortalama bağış analizi
/export - Excel olarak dışa aktar
/export DD.MM.YYYY - DD.MM.YYYY - Tarih aralığı
/analiz [URL] - UTM linkinden bağış analizi

<b>🔗 UTM Komutları:</b>
/build - Yeni UTM link oluştur
/cancel - İşlemi iptal et
/myid - Chat ID'nizi öğrenin

<b>UTM Parametreleri:</b>
• utm_source - Trafik kaynağı
• utm_medium - Pazarlama ortamı
• utm_campaign - Kampanya adı
• utm_content - Kreatif/içerik adı
• utm_term - Reklam seti (opsiyonel)`

	msg := tgbotapi.NewMessage(chatID, welcomeText)
	msg.ParseMode = "HTML"
	bot.Send(msg)
}

// startBuildProcess UTM oluşturma sürecini başlatır
func startBuildProcess(bot *tgbotapi.BotAPI, chatID int64, userID int64) {
	// Yeni session oluştur
	sessionsMutex.Lock()
	sessions[userID] = &UserSession{Step: 1}
	log.Printf("Yeni session oluşturuldu: userID=%d, toplam session=%d", userID, len(sessions))
	sessionsMutex.Unlock()

	msg := tgbotapi.NewMessage(chatID, "📝 *Adım 1/6: Kaynak URL*\n\nLütfen UTM parametreleri eklemek istediğiniz URL'yi girin.\n\nÖrnek: `https://hayratyardim.org/bagis/genel-su-kuyusu/`")
	msg.ParseMode = "Markdown"
	bot.Send(msg)
}

// cancelSession işlemi iptal eder
func cancelSession(bot *tgbotapi.BotAPI, chatID int64, userID int64) {
	sessionsMutex.Lock()
	delete(sessions, userID)
	sessionsMutex.Unlock()

	msg := tgbotapi.NewMessage(chatID, "❌ İşlem iptal edildi. Yeni bir link oluşturmak için /build komutunu kullanabilirsiniz.")
	bot.Send(msg)
}

// handleUserInput kullanıcı girdisini işler
func handleUserInput(bot *tgbotapi.BotAPI, chatID int64, userID int64, text string, session *UserSession) {
	switch session.Step {
	case 1: // Kaynak URL
		// URL validasyonu
		if !isValidURL(text) {
			msg := tgbotapi.NewMessage(chatID, "⚠️ Geçersiz URL formatı. Lütfen geçerli bir URL girin (https:// ile başlamalı).")
			bot.Send(msg)
			return
		}
		session.SourceURL = text
		session.Step = 2
		askUTMSource(bot, chatID)

	case 4: // Kampanya adı
		session.Campaign = sanitizeUTMValue(text)
		session.Step = 5
		msg := tgbotapi.NewMessage(chatID, "📝 *Adım 5/6: Kreatif Adı (utm_content)*\n\nLütfen kreatif/içerik adını girin.\n\n⚠️ *Uyarı:* Türkçe karakter kullanmayın (ş, ı, ğ, ü, ö, ç)\n\nÖrnek: `test_genel_su_kuyusu`")
		msg.ParseMode = "Markdown"
		bot.Send(msg)

	case 5: // Content
		session.Content = sanitizeUTMValue(text)
		session.Step = 6
		askUTMTerm(bot, chatID)

	case 6: // Term (opsiyonel)
		if text != "" && strings.ToLower(text) != "atla" {
			session.Term = sanitizeUTMValue(text)
		}
		// UTM linkini oluştur ve gönder
		sendFinalURL(bot, chatID, userID, session)
	}
}

// handleCallback inline button tıklamalarını işler
func handleCallback(bot *tgbotapi.BotAPI, callback *tgbotapi.CallbackQuery) {
	userID := callback.From.ID
	chatID := callback.Message.Chat.ID
	data := callback.Data

	log.Printf("Callback alındı: userID=%d, chatID=%d, data=%s", userID, chatID, data)

	// Callback'i yanıtla (loading göstergesini kaldır)
	bot.Request(tgbotapi.NewCallback(callback.ID, ""))

	sessionsMutex.RLock()
	session, exists := sessions[userID]
	// Debug: Mevcut session'ları logla
	sessionKeys := make([]int64, 0, len(sessions))
	for k := range sessions {
		sessionKeys = append(sessionKeys, k)
	}
	log.Printf("Mevcut session'lar: %v, aranan userID: %d, bulundu: %v", sessionKeys, userID, exists)
	sessionsMutex.RUnlock()

	if !exists {
		log.Printf("UYARI: Session bulunamadı! userID=%d", userID)
		msg := tgbotapi.NewMessage(chatID, "Oturum bulunamadı. Lütfen /build ile yeniden başlayın.")
		bot.Send(msg)
		return
	}

	log.Printf("Session bulundu: userID=%d, step=%d", userID, session.Step)

	switch session.Step {
	case 2: // UTM Source seçimi
		session.UTMSource = data
		session.Step = 3
		askUTMMedium(bot, chatID)

	case 3: // UTM Medium seçimi
		session.UTMMedium = data
		session.Step = 4
		msg := tgbotapi.NewMessage(chatID, "📝 *Adım 4/6: Kampanya Adı (utm_campaign)*\n\nLütfen kampanya adını girin.\n\n⚠️ *Uyarı:* Türkçe karakter kullanmayın (ş, ı, ğ, ü, ö, ç)\n\nÖrnek: `su_kuyusu_genel`")
		msg.ParseMode = "Markdown"
		bot.Send(msg)

	case 6: // Term skip
		if data == "skip_term" {
			sendFinalURL(bot, chatID, userID, session)
		}
	}
}

// askUTMSource utm_source için inline keyboard gösterir
func askUTMSource(bot *tgbotapi.BotAPI, chatID int64) {
	var rows [][]tgbotapi.InlineKeyboardButton

	// 3'erli satırlar oluştur
	var currentRow []tgbotapi.InlineKeyboardButton
	for i, source := range utmSourceOptions {
		btn := tgbotapi.NewInlineKeyboardButtonData(source, source)
		currentRow = append(currentRow, btn)
		if (i+1)%3 == 0 || i == len(utmSourceOptions)-1 {
			rows = append(rows, currentRow)
			currentRow = []tgbotapi.InlineKeyboardButton{}
		}
	}

	keyboard := tgbotapi.NewInlineKeyboardMarkup(rows...)

	msg := tgbotapi.NewMessage(chatID, "📝 *Adım 2/6: Trafik Kaynağı (utm_source)*\n\nAşağıdaki seçeneklerden birini seçin:")
	msg.ParseMode = "Markdown"
	msg.ReplyMarkup = keyboard
	bot.Send(msg)
}

// askUTMMedium utm_medium için inline keyboard gösterir
func askUTMMedium(bot *tgbotapi.BotAPI, chatID int64) {
	var rows [][]tgbotapi.InlineKeyboardButton

	// 2'şerli satırlar oluştur
	var currentRow []tgbotapi.InlineKeyboardButton
	for i, medium := range utmMediumOptions {
		btn := tgbotapi.NewInlineKeyboardButtonData(medium, medium)
		currentRow = append(currentRow, btn)
		if (i+1)%2 == 0 || i == len(utmMediumOptions)-1 {
			rows = append(rows, currentRow)
			currentRow = []tgbotapi.InlineKeyboardButton{}
		}
	}

	keyboard := tgbotapi.NewInlineKeyboardMarkup(rows...)

	msg := tgbotapi.NewMessage(chatID, "📝 *Adım 3/6: Pazarlama Ortamı (utm_medium)*\n\nAşağıdaki seçeneklerden birini seçin:")
	msg.ParseMode = "Markdown"
	msg.ReplyMarkup = keyboard
	bot.Send(msg)
}

// askUTMTerm utm_term için seçenek sunar
func askUTMTerm(bot *tgbotapi.BotAPI, chatID int64) {
	skipBtn := tgbotapi.NewInlineKeyboardButtonData("⏭️ Atla (Boş Bırak)", "skip_term")
	keyboard := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(skipBtn),
	)

	msg := tgbotapi.NewMessage(chatID, "📝 *Adım 6/6: Reklam Seti (utm_term) - Opsiyonel*\n\nReklam seti adını girin veya boş bırakmak için 'Atla' butonuna tıklayın.\n\n⚠️ *Uyarı:* Türkçe karakter kullanmayın (ş, ı, ğ, ü, ö, ç)")
	msg.ParseMode = "Markdown"
	msg.ReplyMarkup = keyboard
	bot.Send(msg)
}

// sendFinalURL son UTM linkini oluşturur ve gönderir
func sendFinalURL(bot *tgbotapi.BotAPI, chatID int64, userID int64, session *UserSession) {
	// URL'yi parse et
	parsedURL, err := url.Parse(session.SourceURL)
	if err != nil {
		msg := tgbotapi.NewMessage(chatID, "❌ URL işlenirken bir hata oluştu. Lütfen /build ile tekrar deneyin.")
		bot.Send(msg)
		return
	}

	// Mevcut query parametrelerini al
	query := parsedURL.Query()

	// UTM parametrelerini ekle
	query.Set("utm_source", session.UTMSource)
	query.Set("utm_medium", session.UTMMedium)
	query.Set("utm_campaign", session.Campaign)
	query.Set("utm_content", session.Content)
	if session.Term != "" {
		query.Set("utm_term", session.Term)
	}

	// Yeni URL'yi oluştur
	parsedURL.RawQuery = query.Encode()
	finalURL := parsedURL.String()

	// Sonucu gönder (HTML formatında - Markdown'daki _ sorunu için)
	var sb strings.Builder
	sb.WriteString("✅ <b>UTM Link Başarıyla Oluşturuldu!</b>\n\n")
	sb.WriteString("📊 <b>Parametreler:</b>\n")
	sb.WriteString(fmt.Sprintf("• Kaynak URL: %s\n", session.SourceURL))
	sb.WriteString(fmt.Sprintf("• utm_source: %s\n", session.UTMSource))
	sb.WriteString(fmt.Sprintf("• utm_medium: %s\n", session.UTMMedium))
	sb.WriteString(fmt.Sprintf("• utm_campaign: %s\n", session.Campaign))
	sb.WriteString(fmt.Sprintf("• utm_content: %s\n", session.Content))

	if session.Term != "" {
		sb.WriteString(fmt.Sprintf("• utm_term: %s\n", session.Term))
	}

	sb.WriteString(fmt.Sprintf("\n🔗 <b>Son URL:</b>\n<code>%s</code>\n\n", finalURL))
	sb.WriteString("Yeni bir link oluşturmak için /build komutunu kullanabilirsiniz.")

	msg := tgbotapi.NewMessage(chatID, sb.String())
	msg.ParseMode = "HTML"
	if _, err := bot.Send(msg); err != nil {
		log.Printf("Final URL mesajı gönderilemedi: %v", err)
		// Hata olursa düz metin olarak gönder
		plainMsg := tgbotapi.NewMessage(chatID, fmt.Sprintf("✅ UTM Link Oluşturuldu!\n\n%s", finalURL))
		bot.Send(plainMsg)
	}

	// Session'ı temizle
	sessionsMutex.Lock()
	delete(sessions, userID)
	sessionsMutex.Unlock()
}

// isValidURL URL'nin geçerli olup olmadığını kontrol eder
func isValidURL(text string) bool {
	parsedURL, err := url.Parse(text)
	if err != nil {
		return false
	}
	return parsedURL.Scheme == "http" || parsedURL.Scheme == "https"
}

// sanitizeUTMValue UTM değerlerini temizler (boşlukları _ ile değiştirir, Türkçe karakterleri dönüştürür)
func sanitizeUTMValue(value string) string {
	// Boşlukları alt çizgi ile değiştir
	value = strings.ReplaceAll(value, " ", "_")
	// Küçük harfe çevir
	value = strings.ToLower(value)
	// Türkçe karakterleri İngilizce karşılıklarına dönüştür
	value = replaceTurkishChars(value)
	return value
}

// replaceTurkishChars Türkçe karakterleri İngilizce karşılıklarına dönüştürür
func replaceTurkishChars(s string) string {
	replacements := map[rune]rune{
		'ş': 's',
		'Ş': 'S',
		'ı': 'i',
		'İ': 'I',
		'ğ': 'g',
		'Ğ': 'G',
		'ü': 'u',
		'Ü': 'U',
		'ö': 'o',
		'Ö': 'O',
		'ç': 'c',
		'Ç': 'C',
	}

	var result strings.Builder
	for _, r := range s {
		if replacement, ok := replacements[r]; ok {
			result.WriteRune(replacement)
		} else {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// handleKalemCommand /kalem komutunu işler - Bağış kalemi detaylı analizi
func handleKalemCommand(bot *tgbotapi.BotAPI, chatID int64, args string) {
	itemName := strings.TrimSpace(args)

	if itemName == "" {
		// Mevcut bağış kalemlerini listele
		ctx := context.Background()
		var items []struct {
			ItemName string `bun:"item_name"`
		}
		err := db.NewRaw(`
			SELECT DISTINCT item->>'item_name' as item_name
			FROM orders, jsonb_array_elements(items) as item
			ORDER BY item_name
		`).Scan(ctx, &items)

		if err != nil || len(items) == 0 {
			msg := tgbotapi.NewMessage(chatID, "❌ Bağış kalemi bulunamadı.")
			bot.Send(msg)
			return
		}

		var sb strings.Builder
		sb.WriteString("📦 <b>Mevcut Bağış Kalemleri</b>\n\n")
		sb.WriteString("Detay görmek için:\n<code>/kalem [kalem adı]</code>\n\n")
		sb.WriteString("<b>Kalemler:</b>\n")
		for _, item := range items {
			sb.WriteString(fmt.Sprintf("  • %s\n", item.ItemName))
		}

		msg := tgbotapi.NewMessage(chatID, sb.String())
		msg.ParseMode = "HTML"
		bot.Send(msg)
		return
	}

	ctx := context.Background()

	// Türkiye saati için UTC+3
	now := time.Now().UTC().Add(3 * time.Hour)
	startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	endOfDay := startOfDay.Add(24 * time.Hour)
	startOfDayUTC := startOfDay.Add(-3 * time.Hour)
	endOfDayUTC := endOfDay.Add(-3 * time.Hour)

	// 1. Tüm zamanlar toplamı
	var allTimeStats struct {
		Total float64 `bun:"total"`
		Count int     `bun:"count"`
	}
	err := db.NewRaw(`
		SELECT 
			COALESCE(SUM((item->>'price')::numeric * (item->>'quantity')::numeric), 0) as total,
			COALESCE(SUM((item->>'quantity')::numeric), 0)::int as count
		FROM orders, jsonb_array_elements(items) as item
		WHERE item->>'item_name' ILIKE ?
	`, "%"+itemName+"%").Scan(ctx, &allTimeStats)

	if err != nil {
		log.Printf("Kalem tüm zamanlar sorgu hatası: %v", err)
		msg := tgbotapi.NewMessage(chatID, "❌ Veritabanı sorgu hatası oluştu.")
		bot.Send(msg)
		return
	}

	if allTimeStats.Count == 0 {
		msg := tgbotapi.NewMessage(chatID, fmt.Sprintf("❌ <b>%s</b> adında bağış kalemi bulunamadı.", itemName))
		msg.ParseMode = "HTML"
		bot.Send(msg)
		return
	}

	// 2. Bugünkü toplam
	var todayStats struct {
		Total float64 `bun:"total"`
		Count int     `bun:"count"`
	}
	db.NewRaw(`
		SELECT 
			COALESCE(SUM((item->>'price')::numeric * (item->>'quantity')::numeric), 0) as total,
			COALESCE(SUM((item->>'quantity')::numeric), 0)::int as count
		FROM orders, jsonb_array_elements(items) as item
		WHERE item->>'item_name' ILIKE ?
		AND event_time >= ? AND event_time < ?
	`, "%"+itemName+"%", startOfDayUTC, endOfDayUTC).Scan(ctx, &todayStats)

	// 3. Tüm zamanlar kaynak dağılımı
	var allTimeSources []struct {
		Source string  `bun:"source"`
		Total  float64 `bun:"total"`
		Count  int     `bun:"count"`
	}
	db.NewRaw(`
		SELECT 
			CASE 
				WHEN o.utm_source IS NOT NULL AND o.utm_source != '' THEN o.utm_source
				WHEN o.traffic_channel = 'google' THEN 'Google Ads'
				ELSE 'Doğrudan'
			END as source,
			SUM((item->>'price')::numeric * (item->>'quantity')::numeric) as total,
			SUM((item->>'quantity')::numeric)::int as count
		FROM orders o, jsonb_array_elements(o.items) as item
		WHERE item->>'item_name' ILIKE ?
		GROUP BY 1
		ORDER BY total DESC
	`, "%"+itemName+"%").Scan(ctx, &allTimeSources)

	// 4. Bugünkü kaynak dağılımı
	var todaySources []struct {
		Source string  `bun:"source"`
		Total  float64 `bun:"total"`
		Count  int     `bun:"count"`
	}
	db.NewRaw(`
		SELECT 
			CASE 
				WHEN o.utm_source IS NOT NULL AND o.utm_source != '' THEN o.utm_source
				WHEN o.traffic_channel = 'google' THEN 'Google Ads'
				ELSE 'Doğrudan'
			END as source,
			SUM((item->>'price')::numeric * (item->>'quantity')::numeric) as total,
			SUM((item->>'quantity')::numeric)::int as count
		FROM orders o, jsonb_array_elements(o.items) as item
		WHERE item->>'item_name' ILIKE ?
		AND o.event_time >= ? AND o.event_time < ?
		GROUP BY 1
		ORDER BY total DESC
	`, "%"+itemName+"%", startOfDayUTC, endOfDayUTC).Scan(ctx, &todaySources)

	// Raporu oluştur
	gunAdi := getTurkishDayName(now.Weekday())

	var sb strings.Builder
	sb.WriteString("━━━━━━━━━━━━━━━━━━━━━━\n")
	sb.WriteString(fmt.Sprintf("📦 <b>%s</b>\n", strings.ToUpper(itemName)))
	sb.WriteString("━━━━━━━━━━━━━━━━━━━━━━\n\n")

	// Tüm zamanlar
	sb.WriteString("📊 <b>TÜM ZAMANLAR</b>\n")
	sb.WriteString("━━━━━━━━━━━━━━━━━━━━━━\n\n")
	sb.WriteString(fmt.Sprintf("   💵 Toplam Tutar : <b>%.2f TRY</b>\n", allTimeStats.Total))
	sb.WriteString(fmt.Sprintf("   📦 Toplam Adet  : <b>%d</b>\n\n", allTimeStats.Count))

	if len(allTimeSources) > 0 {
		sb.WriteString("   <b>Kaynak Dağılımı:</b>\n")
		for _, s := range allTimeSources {
			percentage := (s.Total / allTimeStats.Total) * 100
			sb.WriteString(fmt.Sprintf("   • %s: %.2f TRY (%d) %%%.1f\n", s.Source, s.Total, s.Count, percentage))
		}
	}
	sb.WriteString("\n")

	// Bugün
	sb.WriteString(fmt.Sprintf("☀️ <b>BUGÜN</b> (%s, %s)\n", now.Format("02.01.2006"), gunAdi))
	sb.WriteString("━━━━━━━━━━━━━━━━━━━━━━\n\n")

	if todayStats.Count == 0 {
		sb.WriteString("   ℹ️ Bugün bu kalemden bağış yok.\n")
	} else {
		sb.WriteString(fmt.Sprintf("   💵 Toplam Tutar : <b>%.2f TRY</b>\n", todayStats.Total))
		sb.WriteString(fmt.Sprintf("   📦 Toplam Adet  : <b>%d</b>\n\n", todayStats.Count))

		if len(todaySources) > 0 {
			sb.WriteString("   <b>Kaynak Dağılımı:</b>\n")
			for _, s := range todaySources {
				percentage := (s.Total / todayStats.Total) * 100
				sb.WriteString(fmt.Sprintf("   • %s: %.2f TRY (%d) %%%.1f\n", s.Source, s.Total, s.Count, percentage))
			}
		}
	}

	sb.WriteString("\n━━━━━━━━━━━━━━━━━━━━━━\n")

	msg := tgbotapi.NewMessage(chatID, sb.String())
	msg.ParseMode = "HTML"
	bot.Send(msg)
}

// handleSourceAnalysisCommand /google ve /meta komutlarını işler - Kaynak bazlı detaylı analiz
func handleSourceAnalysisCommand(bot *tgbotapi.BotAPI, chatID int64, source string) {
	ctx := context.Background()

	// Türkiye saati için UTC+3
	now := time.Now().UTC().Add(3 * time.Hour)
	startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	endOfDay := startOfDay.Add(24 * time.Hour)
	startOfDayUTC := startOfDay.Add(-3 * time.Hour)
	endOfDayUTC := endOfDay.Add(-3 * time.Hour)

	// Kaynak filtresi oluştur
	var sourceFilter string
	var sourceTitle string
	var sourceEmoji string

	switch source {
	case "google":
		sourceFilter = "(utm_source = 'google' OR traffic_channel = 'google')"
		sourceTitle = "GOOGLE ADS"
		sourceEmoji = "🔍"
	case "meta":
		sourceFilter = "utm_source = 'meta'"
		sourceTitle = "META (Facebook/Instagram)"
		sourceEmoji = "📱"
	default:
		sourceFilter = fmt.Sprintf("utm_source = '%s'", source)
		sourceTitle = strings.ToUpper(source)
		sourceEmoji = "📊"
	}

	// 1. Tüm zamanlar - Toplam
	var allTimeTotal struct {
		Total float64 `bun:"total"`
		Count int     `bun:"count"`
	}
	db.NewRaw(fmt.Sprintf(`
		SELECT COALESCE(SUM(amount), 0) as total, COUNT(*) as count
		FROM orders WHERE %s
	`, sourceFilter)).Scan(ctx, &allTimeTotal)

	// 2. Tüm zamanlar - Bağış kalemleri
	var allTimeItems []struct {
		ItemName string  `bun:"item_name"`
		Total    float64 `bun:"total"`
		Count    int     `bun:"count"`
	}
	db.NewRaw(fmt.Sprintf(`
		SELECT 
			item->>'item_name' as item_name,
			SUM((item->>'price')::numeric * (item->>'quantity')::numeric) as total,
			SUM((item->>'quantity')::numeric)::int as count
		FROM orders o, jsonb_array_elements(o.items) as item
		WHERE %s
		GROUP BY item->>'item_name'
		ORDER BY total DESC
	`, sourceFilter)).Scan(ctx, &allTimeItems)

	// 3. Bugün - Toplam
	var todayTotal struct {
		Total float64 `bun:"total"`
		Count int     `bun:"count"`
	}
	db.NewRaw(fmt.Sprintf(`
		SELECT COALESCE(SUM(amount), 0) as total, COUNT(*) as count
		FROM orders WHERE %s AND event_time >= ? AND event_time < ?
	`, sourceFilter), startOfDayUTC, endOfDayUTC).Scan(ctx, &todayTotal)

	// 4. Bugün - Bağış kalemleri
	var todayItems []struct {
		ItemName string  `bun:"item_name"`
		Total    float64 `bun:"total"`
		Count    int     `bun:"count"`
	}
	db.NewRaw(fmt.Sprintf(`
		SELECT 
			item->>'item_name' as item_name,
			SUM((item->>'price')::numeric * (item->>'quantity')::numeric) as total,
			SUM((item->>'quantity')::numeric)::int as count
		FROM orders o, jsonb_array_elements(o.items) as item
		WHERE %s AND o.event_time >= ? AND o.event_time < ?
		GROUP BY item->>'item_name'
		ORDER BY total DESC
	`, sourceFilter), startOfDayUTC, endOfDayUTC).Scan(ctx, &todayItems)

	// Raporu oluştur
	gunAdi := getTurkishDayName(now.Weekday())

	var sb strings.Builder
	sb.WriteString("━━━━━━━━━━━━━━━━━━━━━━\n")
	sb.WriteString(fmt.Sprintf("%s <b>%s</b>\n", sourceEmoji, sourceTitle))
	sb.WriteString("━━━━━━━━━━━━━━━━━━━━━━\n\n")

	// Tüm zamanlar
	sb.WriteString("📊 <b>TÜM ZAMANLAR</b>\n")
	sb.WriteString("━━━━━━━━━━━━━━━━━━━━━━\n\n")

	if allTimeTotal.Count == 0 {
		sb.WriteString("   ℹ️ Bu kaynaktan bağış bulunmuyor.\n\n")
	} else {
		sb.WriteString(fmt.Sprintf("   💵 Toplam Gelir  : <b>%.2f TRY</b>\n", allTimeTotal.Total))
		sb.WriteString(fmt.Sprintf("   🛒 Bağış Sayısı  : <b>%d</b>\n", allTimeTotal.Count))
		sb.WriteString(fmt.Sprintf("   📊 Ortalama      : <b>%.2f TRY</b>\n\n", allTimeTotal.Total/float64(allTimeTotal.Count)))

		if len(allTimeItems) > 0 {
			sb.WriteString("   <b>📦 Bağış Kalemleri:</b>\n")
			for _, item := range allTimeItems {
				sb.WriteString(fmt.Sprintf("   • %s\n", item.ItemName))
				sb.WriteString(fmt.Sprintf("     └ %.2f TRY | %d adet\n", item.Total, item.Count))
			}
		}
	}
	sb.WriteString("\n")

	// Bugün
	sb.WriteString(fmt.Sprintf("☀️ <b>BUGÜN</b> (%s, %s)\n", now.Format("02.01.2006"), gunAdi))
	sb.WriteString("━━━━━━━━━━━━━━━━━━━━━━\n\n")

	if todayTotal.Count == 0 {
		sb.WriteString("   ℹ️ Bugün bu kaynaktan bağış yok.\n")
	} else {
		sb.WriteString(fmt.Sprintf("   💵 Toplam Gelir  : <b>%.2f TRY</b>\n", todayTotal.Total))
		sb.WriteString(fmt.Sprintf("   🛒 Bağış Sayısı  : <b>%d</b>\n", todayTotal.Count))
		sb.WriteString(fmt.Sprintf("   📊 Ortalama      : <b>%.2f TRY</b>\n\n", todayTotal.Total/float64(todayTotal.Count)))

		if len(todayItems) > 0 {
			sb.WriteString("   <b>📦 Bağış Kalemleri:</b>\n")
			for _, item := range todayItems {
				sb.WriteString(fmt.Sprintf("   • %s\n", item.ItemName))
				sb.WriteString(fmt.Sprintf("     └ %.2f TRY | %d adet\n", item.Total, item.Count))
			}
		}
	}

	sb.WriteString("\n━━━━━━━━━━━━━━━━━━━━━━\n")

	msg := tgbotapi.NewMessage(chatID, sb.String())
	msg.ParseMode = "HTML"
	bot.Send(msg)
}
