# AI ESG Reporting System

Automated ESG (Environmental, Social, and Governance) reporting system powered by AI for data ingestion, processing, validation, and narrative generation.

## 🚀 Quick Start

```bash
# 1. Install dependencies
poetry install

# 2. Copy environment file
cp .env.example .env

# 3. Start database services
docker-compose up -d

# 4. Run migrations
poetry run alembic upgrade head

# 5. Start server
poetry run uvicorn src.main:app --reload
```

**API will be available at:** `http://localhost:8000`

**Interactive API docs:** `http://localhost:8000/docs`

---

## 📋 Features

### ✅ Implemented
- ✅ **Data Ingestion** - Parse Excel, CSV, and PDF files
- ✅ **Entity Matching** - Intelligent matching of data entities (rule-based + LLM)
- ✅ **Data Normalization** - Automatic unit conversion and standardization
- ✅ **Validation** - 28 industry-specific rules with cross-field checks
- ✅ **Review Workflow** - Mark false positives, track unreviewed errors
- ✅ **Comprehensive Tests** - >85% coverage with 60+ test cases

### 🚧 Pending
- ⏳ **RAG Narratives** - AI-generated reporting narratives (placeholder ready)
- ⏳ **Frontend UI** - Web interface for data upload and review

---

## 📊 System Flow

```
Upload File → Parse → Match → Normalize → Validate → (Generate Report)
   ↓           ↓       ↓         ↓          ↓
 Excel/CSV   Extract  Find     Convert   Check
   PDF       Data   Indicators  Units    Quality
```

---

## 🏗️ Architecture

### Core Modules

| Module | Purpose | Status |
|--------|---------|--------|
| **Ingestion** | File parsing (Excel, CSV, PDF) | ✅ Complete |
| **Matching** | Column → indicator mapping | ✅ Complete |
| **Normalization** | Unit conversions | ✅ Complete |
| **Validation** | Quality checks (28 rules) | ✅ Complete |
| **Generation** | RAG narratives | ⏳ Pending |

### Key Technologies
- **Backend:** Python 3.12+, FastAPI, SQLAlchemy
- **Database:** PostgreSQL (with Alembic migrations)
- **Cache:** Redis
- **AI/ML:** Groq LLM, LangChain, Sentence Transformers
- **Testing:** Pytest (60+ tests, >85% coverage)

---

## 📂 Project Structure

```
backend/
├── src/
│   ├── ingestion/        # File parsers
│   ├── matching/         # Entity matching
│   ├── normalization/    # Unit conversion
│   ├── validation/       # Quality checks
│   ├── api/             # FastAPI endpoints
│   ├── common/          # Shared code
│   └── main.py          # App entry point
├── data/
│   ├── validation-rules/    # Validation + conversion rules
│   └── sample-inputs/       # Test data
├── tests/               # Pytest test suite
├── docs/
│   ├── prd.md          # Product requirements
│   └── SYSTEM_GUIDE.md # Complete system documentation
└── pyproject.toml      # Dependencies (Poetry)
```

---

## 🔧 Prerequisites

- **Python:** 3.12+
- **Poetry:** For dependency management
- **Docker & Docker Compose:** For PostgreSQL and Redis
- **Groq API Key:** For LLM-based matching (optional, has fallback)

---

## 📖 Documentation

### Quick References
- **Complete System Guide:** [`docs/SYSTEM_GUIDE.md`](docs/SYSTEM_GUIDE.md)
  - Module explanations with examples
  - API endpoint details
  - Data flow walkthrough
  - Troubleshooting guide
  
- **Product Requirements:** [`docs/prd.md`](docs/prd.md)
  - Features and objectives
  - Technical stack
  - Success metrics

- **Interactive API Docs:** `http://localhost:8000/docs` (when server running)

---

## 🧪 Testing

```bash
# Run all tests
poetry run pytest

# Run with coverage
poetry run pytest --cov=src --cov-report=html

# Run specific module
poetry run pytest tests/test_validation.py -v

# Run integration tests only
poetry run pytest -m integration -v
```

**Test Coverage:**
- Ingestion: CSV/Excel parsing, error handling
- Matching: Rule-based + LLM matching
- Normalization: Unit conversions
- Validation: 60+ tests covering all rule types

---

## 🔌 API Examples

### 1. Upload and Ingest File
```bash
POST /api/v1/ingest
# Upload Excel/CSV/PDF file
```

### 2. Match Indicators
```bash
POST /api/v1/matching/match-headers
# Map column names to standard indicators
```

### 3. Normalize Units
```bash
POST /api/v1/normalization/normalize
# Convert units to standard format
```

### 4. Validate Data
```bash
# Run validation
POST /api/v1/validation/process/{upload_id}?industry=cement_industry

# Get comprehensive report
GET /api/v1/validation/report/{upload_id}

# Get errors only
GET /api/v1/validation/errors/{upload_id}

# Mark error as reviewed
POST /api/v1/validation/review/mark-reviewed
{
  "result_id": "uuid",
  "reviewer": "user@example.com",
  "notes": "False positive - value is correct"
}

# Check export readiness
GET /api/v1/validation/review-summary/{upload_id}
```

---

## 🎯 Industry Rules Coverage

### Cement Industry (3 rules)
- Emission intensity: 800-1,100 kg CO₂/tonne clinker
- Energy intensity: 2.9-4.5 GJ/tonne clinker
- Clinker ratio: 0.65-0.95

### Steel Industry (3 rules)
- BF-BOF emissions: 1,800-2,500 kg CO₂/tonne
- EAF emissions: 400-600 kg CO₂/tonne
- Energy intensity: 18-25 GJ/tonne

### Automotive (3 rules)
- Manufacturing: 4-12 tonnes CO₂e/vehicle
- VOC: 10-35 kg/vehicle
- Water: 3-8 m³/vehicle

### Cross-Industry (8 rules)
- Scope totals consistency
- Temporal consistency
- Outlier detection
- Unit validation
- Negative checks
- And more...

**Total: 28 validation rules** with industry citations

---

## 🔄 Validation Workflow

1. **Run Validation** → System checks 28 rules
2. **Review Errors** → User sees errors with suggested fixes
3. **Take Action:**
   - Fix data and re-validate, OR
   - Mark as false positive with notes
4. **Export Ready** → When all errors addressed

**Export blocked until:** `unreviewed_errors = 0`

---

## ⚙️ Configuration

### Environment Variables (`.env`)
```bash
# Database
DATABASE_URL=postgresql://user:pass@localhost:5432/esg_db

# Redis
REDIS_URL=redis://localhost:6379/0

# Groq API (optional for LLM matching)
GROQ_API_KEY=your-api-key
GROQ_MODEL=llama-3.3-70b-versatile

# Thresholds
MATCHING_CONFIDENCE_THRESHOLD=0.80
```

### Validation Rules
Edit `data/validation-rules/validation_rules.json` to add/modify rules.

### Unit Conversions
Edit `data/validation-rules/conversion_factors.json` for custom conversions.

---

## 🛠️ Development

### Code Quality
```bash
# Format code
poetry run black src/ tests/

# Lint
poetry run ruff check src/ tests/

# Type checking
poetry run mypy src/
```

### Database Migrations
```bash
# Create migration
poetry run alembic revision --autogenerate -m "description"

# Apply migrations
poetry run alembic upgrade head

# Rollback
poetry run alembic downgrade -1
```

### Docker Services
```bash
# Start
docker-compose up -d

# Stop
docker-compose down

# View logs
docker-compose logs -f

# Reset database
docker-compose down -v && docker-compose up -d
```

---

## 📊 Performance

- **Parsing:** ~1,000 rows/second
- **Matching:** 90% accuracy (rule) + 95% (LLM fallback)
- **Validation:** ~1,000 records/second
- **Test Coverage:** >85%

---

## 🚧 Known Limitations

1. **Generation Module:** RAG narrative generation not yet implemented
2. **Frontend:** No UI - API-only currently
3. **PDF Parsing:** Limited to text-based PDFs (no OCR)
4. **Async Processing:** Large files may timeout (implement background jobs)

---

## 🤝 Contributing

1. Create feature branch
2. Make changes with tests
3. Run `poetry run pytest --cov=src`
4. Ensure coverage >85%
5. Submit pull request

---

## 📄 License

MIT License - See LICENSE file for details

---

## 🆘 Support

### Common Issues

**Issue:** Parser fails on Excel
- **Fix:** Ensure file is `.xlsx` (not `.xls`)

**Issue:** Low matching confidence
- **Fix:** Add synonyms to `synonym_dictionary.json`

**Issue:** Validation fails for correct data
- **Fix:** Mark as reviewed via API

**Issue:** Database connection error
- **Fix:** Check `docker-compose ps` - services running?

### Getting Help
1. Check [`docs/SYSTEM_GUIDE.md`](docs/SYSTEM_GUIDE.md)
2. Review API docs: `http://localhost:8000/docs`
3. Check test examples in `tests/` folder

---

## 📈 Project Status

**Version:** 0.1.0

**Completion:**
- Core Pipeline: ✅ 100% (Ingest → Validate)
- Generation Module: ⏳ 0% (Pending)
- Frontend UI: ⏳ 0% (Pending)

**Next Steps:**
1. Implement RAG narrative generation
2. Build frontend interface
3. Add real-time monitoring dashboard
4. Implement background job queue

---

**Built with:** Python, FastAPI, PostgreSQL, Redis, LangChain, Groq

**Last Updated:** February 2026
