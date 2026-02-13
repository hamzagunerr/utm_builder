package main

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

// getBotToken environment variable'dan bot token'ı alır
func getBotToken() string {
	token := os.Getenv("TELEGRAM_BOT_TOKEN")
	if token == "" {
		log.Fatal("TELEGRAM_BOT_TOKEN environment variable is not set")
	}
	return token
}

// UserSession kullanıcının UTM oluşturma sürecindeki durumunu tutar
type UserSession struct {
	Step       int    // Hangi adımda olduğu (1-6)
	SourceURL  string // Kaynak URL
	UTMSource  string // utm_source
	UTMMedium  string // utm_medium
	Campaign   string // utm_campaign
	Content    string // utm_content
	Term       string // utm_term (opsiyonel)
}

// sessions tüm kullanıcı oturumlarını tutar
var sessions = make(map[int64]*UserSession)
var sessionsMutex sync.RWMutex

// UTM Source seçenekleri
var utmSourceOptions = []string{"meta", "google", "tiktok", "linkedin", "sms", "email", "x"}

// UTM Medium seçenekleri
var utmMediumOptions = []string{"paid_social", "cpc", "display", "paid_search", "sms", "email", "organic_social"}

func main() {
	// Bot'u oluştur
	bot, err := tgbotapi.NewBotAPI(getBotToken())
	if err != nil {
		log.Panic(err)
	}

	bot.Debug = false
	log.Printf("Bot başlatıldı: @%s", bot.Self.UserName)

	// Update config
	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	updates := bot.GetUpdatesChan(u)

	for update := range updates {
		// Callback query (inline button tıklaması)
		if update.CallbackQuery != nil {
			handleCallback(bot, update.CallbackQuery)
			continue
		}

		// Normal mesaj
		if update.Message != nil {
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
		switch message.Command() {
		case "start":
			sendWelcomeMessage(bot, chatID)
		case "build":
			startBuildProcess(bot, chatID, userID)
		case "cancel":
			cancelSession(bot, chatID, userID)
		default:
			msg := tgbotapi.NewMessage(chatID, "Bilinmeyen komut. /start veya /build komutlarını kullanabilirsiniz.")
			bot.Send(msg)
		}
		return
	}

	// Aktif session varsa, kullanıcı girdisini işle
	sessionsMutex.RLock()
	session, exists := sessions[userID]
	sessionsMutex.RUnlock()

	if exists {
		handleUserInput(bot, chatID, userID, message.Text, session)
	} else {
		msg := tgbotapi.NewMessage(chatID, "UTM link oluşturmak için /build komutunu kullanın.")
		bot.Send(msg)
	}
}

// sendWelcomeMessage hoş geldin mesajı gönderir
func sendWelcomeMessage(bot *tgbotapi.BotAPI, chatID int64) {
	welcomeText := `🔗 *Hayrat Yardım UTM Builder Bot'a Hoş Geldiniz!*

Bu bot, pazarlama kampanyalarınız için UTM parametreli linkler oluşturmanıza yardımcı olur.

*Kullanılabilir Komutlar:*
/build - Yeni UTM link oluştur
/cancel - İşlemi iptal et

*UTM Parametreleri:*
• utm_source - Trafik kaynağı (meta, google, vb.)
• utm_medium - Pazarlama ortamı (paid_social, cpc, vb.)
• utm_campaign - Kampanya adı
• utm_content - Kreatif/içerik adı
• utm_term - Reklam seti (opsiyonel)

Başlamak için /build komutunu kullanın!`

	msg := tgbotapi.NewMessage(chatID, welcomeText)
	msg.ParseMode = "Markdown"
	bot.Send(msg)
}

// startBuildProcess UTM oluşturma sürecini başlatır
func startBuildProcess(bot *tgbotapi.BotAPI, chatID int64, userID int64) {
	// Yeni session oluştur
	sessionsMutex.Lock()
	sessions[userID] = &UserSession{Step: 1}
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

	// Callback'i yanıtla (loading göstergesini kaldır)
	bot.Request(tgbotapi.NewCallback(callback.ID, ""))

	sessionsMutex.RLock()
	session, exists := sessions[userID]
	sessionsMutex.RUnlock()

	if !exists {
		msg := tgbotapi.NewMessage(chatID, "Oturum bulunamadı. Lütfen /build ile yeniden başlayın.")
		bot.Send(msg)
		return
	}

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

	// Sonucu gönder
	resultText := fmt.Sprintf(`✅ *UTM Link Başarıyla Oluşturuldu!*

📊 *Parametreler:*
• Kaynak URL: %s
• utm_source: %s
• utm_medium: %s
• utm_campaign: %s
• utm_content: %s`,
		session.SourceURL,
		session.UTMSource,
		session.UTMMedium,
		session.Campaign,
		session.Content)

	if session.Term != "" {
		resultText += fmt.Sprintf("\n• utm_term: %s", session.Term)
	}

	resultText += fmt.Sprintf("\n\n🔗 *Son URL:*\n`%s`\n\nYeni bir link oluşturmak için /build komutunu kullanabilirsiniz.", finalURL)

	msg := tgbotapi.NewMessage(chatID, resultText)
	msg.ParseMode = "Markdown"
	bot.Send(msg)

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
