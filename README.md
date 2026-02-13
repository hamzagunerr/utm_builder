# Hayrat Yardım UTM Builder Bot

Telegram botu ile UTM parametreli linkler oluşturun.

## Özellikler

- Adım adım UTM link oluşturma
- Inline keyboard ile kolay seçim
- utm_source, utm_medium, utm_campaign, utm_content ve utm_term desteği
- URL validasyonu
- Otomatik küçük harf ve boşluk düzeltme
- Türkçe karakter otomatik dönüşümü

## Kurulum

### Lokal Çalıştırma

```bash
# Bağımlılıkları yükle
go mod download

# Derle
go build -o utm-builder-bot .

# Environment variable ayarla ve çalıştır
export TELEGRAM_BOT_TOKEN="your-bot-token-here"
./utm-builder-bot
```

### Docker ile Çalıştırma

```bash
# Image oluştur
docker build -t utm-builder-bot .

# Container çalıştır
docker run -d --name utm-bot \
  -e TELEGRAM_BOT_TOKEN="your-bot-token-here" \
  --restart unless-stopped \
  utm-builder-bot
```

## Kullanım

1. Telegram'da [@hy_utm_builder_bot](https://t.me/hy_utm_builder_bot) botunu açın
2. `/start` komutu ile başlayın
3. `/build` komutu ile yeni UTM link oluşturun
4. Adımları takip edin:
   - Kaynak URL girin
   - utm_source seçin (meta, google, tiktok, linkedin, sms, email, x)
   - utm_medium seçin (paid_social, cpc, display, paid_search, sms, email, organic_social)
   - Kampanya adı girin
   - Kreatif adı girin
   - Reklam seti girin (opsiyonel)
5. Oluşturulan UTM linkini kopyalayın

## Örnek Çıktı

```
https://hayratyardim.org/bagis/genel-su-kuyusu/?utm_source=meta&utm_medium=organic_social&utm_campaign=su_kuyusu_genel&utm_content=test_genel_su_kuyusu
```

## Komutlar

| Komut | Açıklama |
|-------|----------|
| `/start` | Hoş geldin mesajı |
| `/build` | Yeni UTM link oluştur |
| `/cancel` | İşlemi iptal et |

## Environment Variables

| Değişken | Açıklama | Zorunlu |
|----------|----------|---------|
| `TELEGRAM_BOT_TOKEN` | Telegram Bot API Token | Evet |

## GitHub Actions

Her `main` branch'e push yapıldığında otomatik olarak Docker image build edilip Docker Hub'a push edilir.

### Gerekli Secrets

GitHub repo ayarlarında şu secrets'ları tanımlayın:

- `DOCKER_USERNAME` - Docker Hub kullanıcı adı
- `DOCKER_TOKEN` - Docker Hub access token

## Bot Bilgileri

- **Bot Username:** @hy_utm_builder_bot
- **Geliştirici:** Hayrat Yardım
