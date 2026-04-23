const fs = await import("node:fs/promises");
const path = await import("node:path");
const { fileURLToPath } = await import("node:url");
const { Presentation, PresentationFile } = await import("@oai/artifact-tool");

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = path.resolve(__dirname, "..", "..", "..");
const TARGET_PPTX = path.resolve(ROOT_DIR, "..", "самсунг.pptx");
const OUT_DIR = path.dirname(TARGET_PPTX);
const SCRATCH_DIR = path.join(ROOT_DIR, "tmp", "slides", "samsung");
const PREVIEW_DIR = path.join(SCRATCH_DIR, "preview");
const VERIFICATION_DIR = path.join(SCRATCH_DIR, "verification");
const INSPECT_PATH = path.join(SCRATCH_DIR, "inspect.ndjson");
const QR_PATH = path.join(SCRATCH_DIR, "github_qr.png");
const PLAN_PATH = path.join(OUT_DIR, "narrative_plan.md");
const MAX_RENDER_VERIFY_LOOPS = 3;

const W = 1280;
const H = 720;

const C = {
  bg: "#FFFFFF",
  bg2: "#F7FBFF",
  panel: "#FFFFFF",
  panel2: "#F4F9FF",
  line: "#C9DDF1",
  text: "#0B1F33",
  muted: "#5E718A",
  faint: "#8A9DB2",
  blue: "#1F6FE5",
  blue2: "#53A9FF",
  blue3: "#2E8CF0",
  blue4: "#DCEBFF",
  green: "#1EA97C",
  red: "#D94B4B",
  navy: "#EAF4FF",
};

const F = {
  title: "Poppins",
  body: "Lato",
  mono: "Aptos Mono",
};

const SOURCES = {
  repo: "Локальный репозиторий Ice Monitor и Docker Compose.",
  api: "api/main.py: /stats, /pipeline_status, /system_status, WebSocket-стримы.",
  frontend: "frontend/app.py: Streamlit-панель, карточки камер, сервисы, логи.",
  ingest: "ingest/main.py: чтение видео, Redis Streams, Kafka topic ice.frames.*.",
  inference: "inference/worker.py: YOLOv11s-seg, маски, ice_conc, ice_severity.",
  spark: "spark_jobs/kafka_to_hdfs.py и spark_jobs/etl_to_hive.py.",
  mllib: "spark_jobs/mllib_danger_model.py и notebooks/scaling_experiment.ipynb.",
  bench: "benchmarks/hotpath/hotpath_summary.csv и scaling notebook outputs.",
};

const SLIDES = [
  {
    kind: "cover",
    kicker: "Ice Monitor",
    title: "Big Data платформа для мониторинга ледовой обстановки",
    subtitle:
      "Live-контур для оператора, batch-контур для истории и аналитики, Spark-эксперименты для доказательства масштабируемости.",
  },
  {
    kind: "problem",
    kicker: "Проблема",
    title: "Почему проект нужен",
    subtitle: "Данные приходят непрерывно, а решение нужно принимать быстро и на основе истории, а не только live-кадра.",
    cards: [
      ["Сложная среда", "Лёд, шуга и суда пересекаются в одном кадре, поэтому оператору нужен не просто поток видео, а готовая аналитика."],
      ["Поток событий", "Две камеры генерируют кадры, детекции и метрики. Ручной просмотр быстро перестаёт работать."],
      ["История и отчёты", "Нужны хранение, SQL-аналитика, BI и ML по накопленным данным, а не только картинка в реальном времени."],
    ],
  },
  {
    kind: "solution",
    kicker: "Решение",
    title: "Что делает система",
    subtitle: "Проект разделяет live и batch, чтобы не мешать оператору и сохранять полноценную историю данных.",
    cards: [
      ["Live-контур", "Ingest читает видео, inference строит маски и метрики, API отдаёт WebSocket-стрим, а Streamlit показывает результат."],
      ["Batch-контур", "Kafka события попадают в HDFS/Parquet, затем Spark ETL загружает их в Hive для отчётов и выборок."],
      ["Аналитический слой", "Superset, Jupyter и Spark MLlib работают поверх Hive и превращают поток событий в показатели и эксперименты."],
    ],
  },
  {
    kind: "goals",
    kicker: "Цель и задачи",
    title: "Цель проекта и основные шаги",
    subtitle: "Цель сформулирована как законченное Big Data-решение, а не как отдельный ML-скрипт.",
  },
  {
    kind: "features",
    kicker: "Функции",
    title: "Основные функции системы",
    subtitle: "Ниже собраны ключевые сценарии, которые покрывает платформа.",
  },
  {
    kind: "audience",
    kicker: "Аудитория",
    title: "Для кого это сделано",
    subtitle: "Платформа полезна сразу нескольким ролям, и у каждой свой рабочий сценарий.",
  },
  {
    kind: "scenario",
    kicker: "Сценарий",
    title: "Как пользователь работает с системой",
    subtitle: "Короткий путь от запуска панели до истории, аналитики и ML.",
  },
  {
    kind: "comparison",
    kicker: "Сравнение",
    title: "Сравнение с аналогами",
    subtitle: "Сильная сторона проекта в том, что он закрывает и live, и историю, и аналитику.",
  },
  {
    kind: "software",
    kicker: "Инструменты",
    title: "Выбор программных средств и назначение",
    subtitle: "Стек выбран так, чтобы закрыть live-контур, историческое хранение и аналитику без лишних слоёв.",
  },
  {
    kind: "architecture",
    kicker: "Архитектура",
    title: "Архитектура проекта",
    subtitle: "Два контура работают параллельно: live-сегментация и пакетный сбор данных для истории.",
  },
  {
    kind: "data",
    kicker: "Данные",
    title: "Сведения о данных",
    subtitle: "Показываем, откуда приходят данные, как они устроены и почему их можно считать большими.",
  },
  {
    kind: "algorithms",
    kicker: "Алгоритмы",
    title: "Алгоритмы внутри конвейера и масштабирование",
    subtitle: "Здесь важно не только качество алгоритмов, но и то, что время обработки растёт предсказуемо при росте объёма.",
  },
  {
    kind: "dashboard",
    kicker: "Дашборд",
    title: "Разработка и назначение дашборда",
    subtitle: "Интерфейс оператора собирает live-метрики, статусы и историю в одном экране.",
  },
  {
    kind: "demo",
    kicker: "Демонстрация",
    title: "Что запускать на защите",
    subtitle: "Слайд специально оставлен как рабочая инструкция: что показать и что можно вставить как скрипт.",
  },
  {
    kind: "results",
    kicker: "Результаты",
    title: "Результаты и выводы",
    subtitle: "Основные цифры показывают, что система работает и даёт измеримый эффект.",
  },
  {
    kind: "roadmap",
    kicker: "Развитие",
    title: "Планы по развитию и масштабированию",
    subtitle: "Следующий шаг — добавить устойчивость, больше данных и лучшее качество аналитики.",
  },
  {
    kind: "contacts",
    kicker: "QR и контакты",
    title: "GitHub и контакты",
    subtitle: "На месте QR-кода можно вставить ссылку на репозиторий. Если URL не был задан, оставлен редактируемый плейсхолдер.",
  },
];

const INSPECT = [];

function hexAlpha(color, alpha) {
  const hex = Math.max(0, Math.min(255, Math.round(alpha * 255))).toString(16).padStart(2, "0").toUpperCase();
  return `${color}${hex}`;
}

function lineCfg(fill = "#00000000", width = 0, style = "solid") {
  return { style, fill, width };
}

function textOf(value) {
  return Array.isArray(value) ? value.join("\n") : String(value ?? "");
}

function lineCount(value) {
  const v = textOf(value).trim();
  return v ? v.split(/\n/).length : 0;
}

async function exists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

async function ensureDirs() {
  await fs.mkdir(OUT_DIR, { recursive: true });
  await fs.mkdir(SCRATCH_DIR, { recursive: true });
  await fs.mkdir(PREVIEW_DIR, { recursive: true });
  await fs.mkdir(VERIFICATION_DIR, { recursive: true });
}

async function readImageBlob(imagePath) {
  const bytes = await fs.readFile(imagePath);
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
}

function recordText(slideNo, shape, role, text, x, y, w, h) {
  const value = textOf(text);
  INSPECT.push({
    kind: "textbox",
    slide: slideNo,
    role,
    text: value,
    textChars: value.length,
    textLines: lineCount(value),
    bbox: [x, y, w, h],
    id: shape?.id || null,
  });
}

function recordShape(slideNo, shape, role, x, y, w, h) {
  INSPECT.push({
    kind: "shape",
    slide: slideNo,
    role,
    bbox: [x, y, w, h],
    id: shape?.id || null,
  });
}

function addShape(slide, geometry, x, y, w, h, fill = "#00000000", line = "#00000000", lineWidth = 0, meta = {}) {
  const shape = slide.shapes.add({
    geometry,
    position: { left: x, top: y, width: w, height: h },
    fill,
    line: lineCfg(line, lineWidth),
  });
  if (meta.role) recordShape(meta.slideNo, shape, meta.role, x, y, w, h);
  return shape;
}

function addText(slide, slideNo, text, x, y, w, h, opts = {}) {
  const box = addShape(
    slide,
    "rect",
    x,
    y,
    w,
    h,
    opts.fill || "#00000000",
    opts.line || "#00000000",
    opts.lineWidth || 0,
    { slideNo, role: opts.role || "text" },
  );
  box.text = text;
  box.text.fontSize = opts.size || 18;
  box.text.color = opts.color || C.text;
  box.text.bold = Boolean(opts.bold);
  box.text.typeface = opts.face || F.body;
  box.text.alignment = opts.align || "left";
  box.text.verticalAlignment = opts.valign || "top";
  box.text.insets = { left: 0, right: 0, top: 0, bottom: 0 };
  box.text.autoFit = opts.autoFit || "shrinkText";
  recordText(slideNo, box, opts.role || "text", text, x, y, w, h);
  return box;
}

function addPanel(slide, slideNo, x, y, w, h, accent = C.blue, fill = C.panel, line = C.line) {
  addShape(slide, "roundRect", x, y, w, h, fill, line, 1.1, { slideNo, role: "panel" });
  addShape(slide, "rect", x, y, w, 7, accent, "#00000000", 0, { slideNo, role: "panel accent" });
}

function addChip(slide, slideNo, text, x, y, w, h, accent = C.blue, fill = C.navy, color = C.blue) {
  addShape(slide, "roundRect", x, y, w, h, fill, accent, 1, { slideNo, role: "chip" });
  addText(slide, slideNo, text, x + 8, y + 3, w - 16, h - 4, {
    size: 10,
    color,
    bold: true,
    face: F.mono,
    align: "center",
    valign: "middle",
    role: "chip text",
  });
}

function addGlow(slide, x, y, size, color, alpha = 0.16) {
  addShape(slide, "ellipse", x, y, size, size, hexAlpha(color, alpha), "#00000000", 0);
}

function addBackground(slide) {
  slide.background.fill = C.bg;
  addGlow(slide, 980, -80, 280, C.blue2, 0.20);
  addGlow(slide, -120, 90, 330, C.blue3, 0.10);
  addGlow(slide, 1040, 510, 240, C.blue4, 0.55);
  addGlow(slide, 160, 530, 200, C.blue4, 0.55);
  for (let i = 0; i < 5; i += 1) {
    addShape(slide, "rect", 150 + i * 230, 56, 1, 610, "#E2ECF8", "#00000000", 0);
  }
}

function addHeader(slide, slideNo, kicker, title, subtitle) {
  addText(slide, slideNo, String(kicker).toUpperCase(), 60, 28, 280, 20, {
    size: 12,
    color: C.blue,
    bold: true,
    face: F.mono,
    role: "kicker",
  });
  addText(slide, slideNo, `${String(slideNo).padStart(2, "0")} / ${String(SLIDES.length).padStart(2, "0")}`, 1120, 28, 80, 20, {
    size: 12,
    color: C.blue,
    bold: true,
    face: F.mono,
    align: "right",
    role: "page number",
  });
  addShape(slide, "rect", 60, 56, 1160, 2, C.line, "#00000000", 0, { slideNo, role: "header line" });
  addText(slide, slideNo, title, 60, 82, 820, 90, {
    size: 34,
    color: C.text,
    bold: true,
    face: F.title,
    role: "title",
  });
  addText(slide, slideNo, subtitle, 60, 182, 780, 56, {
    size: 16,
    color: C.muted,
    face: F.body,
    role: "subtitle",
  });
}

function addCard(slide, slideNo, x, y, w, h, title, body, accent = C.blue, opts = {}) {
  addPanel(slide, slideNo, x, y, w, h, accent, opts.fill || C.panel, opts.line || C.line);
  addText(slide, slideNo, title, x + 22, y + 18, w - 44, 30, {
    size: opts.titleSize || 16,
    color: C.text,
    bold: true,
    face: F.title,
    role: opts.titleRole || "card title",
  });
  addText(slide, slideNo, body, x + 22, y + 54, w - 44, h - 70, {
    size: opts.bodySize || 13,
    color: C.muted,
    face: F.body,
    role: opts.bodyRole || "card body",
  });
}

function addMetricCard(slide, slideNo, x, y, w, h, metric, label, note, accent = C.blue) {
  addPanel(slide, slideNo, x, y, w, h, accent, C.panel2, C.line);
  addText(slide, slideNo, metric, x + 18, y + 18, w - 36, 42, {
    size: 30,
    color: C.text,
    bold: true,
    face: F.title,
    role: "metric value",
  });
  addText(slide, slideNo, label, x + 18, y + 62, w - 36, 28, {
    size: 13,
    color: C.blue,
    bold: true,
    face: F.mono,
    role: "metric label",
  });
  if (note && h >= 140) {
    addText(slide, slideNo, note, x + 18, y + 96, w - 36, h - 112, {
      size: 11,
      color: C.muted,
      face: F.body,
      role: "metric note",
    });
  }
}

function addBulletList(slide, slideNo, x, y, w, bullets, accent = C.blue) {
  bullets.forEach((bullet, idx) => {
    const top = y + idx * 34;
    addShape(slide, "ellipse", x, top + 4, 12, 12, accent, "#00000000", 0, { slideNo, role: "bullet dot" });
    addText(slide, slideNo, bullet, x + 20, top, w - 20, 30, {
      size: 12,
      color: C.text,
      face: F.body,
      role: "bullet",
    });
  });
}

function addTimeline(slide, slideNo, x, y, stepW, steps, accent = C.blue) {
  steps.forEach((step, idx) => {
    const left = x + idx * (stepW + 16);
    addPanel(slide, slideNo, left, y, stepW, 118, accent, C.panel, C.line);
    addText(slide, slideNo, String(idx + 1).padStart(2, "0"), left + 14, y + 12, 36, 24, {
      size: 16,
      color: accent,
      bold: true,
      face: F.mono,
      role: "timeline number",
    });
    addText(slide, slideNo, step.title, left + 14, y + 38, stepW - 28, 24, {
      size: 13,
      color: C.text,
      bold: true,
      face: F.title,
      role: "timeline title",
    });
    addText(slide, slideNo, step.body, left + 14, y + 64, stepW - 28, 40, {
      size: 10,
      color: C.muted,
      face: F.body,
      role: "timeline body",
    });
    if (idx < steps.length - 1) {
      addText(slide, slideNo, "→", left + stepW + 2, y + 42, 12, 24, {
        size: 22,
        color: C.blue2,
        bold: true,
        face: F.title,
        align: "center",
        role: "arrow",
      });
    }
  });
}

function addComparisonGrid(slide, slideNo, x, y, colW, rowH, headers, rows) {
  headers.forEach((head, colIdx) => {
    const left = x + colIdx * colW;
    addPanel(slide, slideNo, left, y, colW - 6, rowH, C.blue, colIdx === 0 ? C.panel2 : C.panel, C.line);
    addText(slide, slideNo, head, left + 10, y + 10, colW - 26, rowH - 16, {
      size: 12,
      color: C.text,
      bold: true,
      face: F.mono,
      align: "center",
      valign: "middle",
      role: "comparison header",
    });
  });
  rows.forEach((row, rowIdx) => {
    const top = y + (rowIdx + 1) * rowH;
    row.forEach((cell, colIdx) => {
      const left = x + colIdx * colW;
      const status = cell === "✓" ? "yes" : cell === "△" ? "partial" : cell === "✗" ? "no" : "text";
      const fill = colIdx === 0 ? C.panel2 : status === "yes" ? "#EAF6FF" : status === "partial" ? "#F4F8FF" : "#FFF5F5";
      const color = colIdx === 0 ? C.text : status === "yes" ? C.green : status === "partial" ? C.blue2 : C.red;
      addPanel(slide, slideNo, left, top, colW - 6, rowH - 6, C.blue, fill, C.line);
      addText(slide, slideNo, cell, left + 10, top + 10, colW - 24, rowH - 20, {
        size: colIdx === 0 ? 11 : 16,
        color,
        bold: colIdx !== 0,
        face: colIdx === 0 ? F.body : F.title,
        align: colIdx === 0 ? "left" : "center",
        valign: "middle",
        role: "comparison cell",
      });
    });
  });
}

async function addLineChart(slide, slideNo, x, y, w, h, title, categories, seriesDefs) {
  addPanel(slide, slideNo, x, y, w, h, C.blue, C.panel, C.line);
  addText(slide, slideNo, title, x + 18, y + 14, w - 36, 24, {
    size: 15,
    color: C.text,
    bold: true,
    face: F.title,
    role: "chart title",
  });
  const chart = slide.charts.add("line");
  chart.position = { left: x + 16, top: y + 42, width: w - 32, height: h - 54 };
  chart.title = "";
  chart.categories = categories;
  chart.hasLegend = true;
  chart.legend.position = "bottom";
  chart.lineOptions.grouping = "standard";
  chart.lineOptions.smooth = false;
  seriesDefs.forEach((def) => {
    const series = chart.series.add(def.name);
    series.values = def.values;
    series.categories = categories;
    series.fill = def.color;
    series.stroke = { width: 2, style: "solid", fill: def.color };
  });
  return chart;
}

function setSpeakerNotes(slide, body, sourceKeys = []) {
  const sourceText = sourceKeys.map((k) => SOURCES[k] || k).join("\n");
  slide.speakerNotes.setText(`${body}\n\n[Sources]\n${sourceText}`);
}

function buildArchitectureText() {
  return [
    ["Камеры", "cam_a / cam_b"],
    ["ingest", "кадры в Redis и Kafka"],
    ["inference", "YOLOv11s-seg, маски, метрики"],
    ["API + Streamlit", "live UI и WebSocket"],
    ["TimescaleDB", "история и состояние"],
  ];
}

async function slideCover(presentation) {
  const slideNo = 1;
  const slide = presentation.slides.add();
  addBackground(slide);
  addShape(slide, "rect", 64, 92, 8, 512, C.blue, "#00000000", 0, { slideNo, role: "cover accent" });
  addText(slide, slideNo, SLIDES[0].kicker, 88, 106, 300, 20, {
    size: 12,
    color: C.blue,
    bold: true,
    face: F.mono,
    role: "cover kicker",
  });
  addText(slide, slideNo, SLIDES[0].title, 88, 150, 700, 170, {
    size: 40,
    color: C.text,
    bold: true,
    face: F.title,
    role: "cover title",
  });
  addText(slide, slideNo, SLIDES[0].subtitle, 88, 330, 640, 84, {
    size: 18,
    color: C.muted,
    face: F.body,
    role: "cover subtitle",
  });
  addChip(slide, slideNo, "REAL-TIME", 88, 454, 112, 28, C.blue, C.navy, C.blue);
  addChip(slide, slideNo, "BATCH", 210, 454, 90, 28, C.blue3, C.navy, C.blue3);
  addChip(slide, slideNo, "BIG DATA", 310, 454, 112, 28, C.blue2, C.navy, C.blue2);
  addPanel(slide, slideNo, 820, 120, 350, 340, C.blue, C.bg2, C.line);
  addText(slide, slideNo, "Поток и история", 846, 146, 220, 24, {
    size: 18,
    color: C.text,
    bold: true,
    face: F.title,
    role: "cover side title",
  });
  addBulletList(
    slide,
    slideNo,
    846,
    190,
    256,
    [
      "Две камеры и два data path.",
      "YOLO-сегментация в hot path.",
      "Kafka, HDFS, Hive и Spark для истории.",
      "BI, Jupyter и MLlib для анализа.",
    ],
    C.blue,
  );
  addMetricCard(slide, slideNo, 88, 524, 180, 110, "2", "КАМЕРЫ", "Непрерывный live-поток из двух источников.", C.blue);
  addMetricCard(slide, slideNo, 284, 524, 202, 110, "4", "КЛАССА", "vessel, ice_field, broken_ice, slush_ice.", C.blue3);
  addMetricCard(slide, slideNo, 502, 524, 234, 110, "2", "КОНТУРА", "Hot path и cold path работают параллельно.", C.blue2);
}

async function slideProblem(presentation) {
  const slideNo = 2;
  const slide = presentation.slides.add();
  addBackground(slide);
  addHeader(slide, slideNo, SLIDES[1].kicker, SLIDES[1].title, SLIDES[1].subtitle);
  const cards = SLIDES[1].cards;
  addCard(slide, slideNo, 60, 278, 360, 304, cards[0][0], cards[0][1], C.blue);
  addCard(slide, slideNo, 460, 278, 360, 304, cards[1][0], cards[1][1], C.blue3);
  addCard(slide, slideNo, 860, 278, 360, 304, cards[2][0], cards[2][1], C.blue2);
  addText(slide, slideNo, "Проблема не в отсутствии видео, а в том, что видео без обработки не помогает принимать решение достаточно быстро.", 60, 656, 1100, 16, {
    size: 10,
    color: C.faint,
    face: F.body,
    role: "footer",
  });
}

async function slideSolution(presentation) {
  const slideNo = 3;
  const slide = presentation.slides.add();
  addBackground(slide);
  addHeader(slide, slideNo, SLIDES[2].kicker, SLIDES[2].title, SLIDES[2].subtitle);
  addCard(slide, slideNo, 60, 278, 360, 304, SLIDES[2].cards[0][0], SLIDES[2].cards[0][1], C.blue);
  addCard(slide, slideNo, 460, 278, 360, 304, SLIDES[2].cards[1][0], SLIDES[2].cards[1][1], C.blue3);
  addCard(slide, slideNo, 860, 278, 360, 304, SLIDES[2].cards[2][0], SLIDES[2].cards[2][1], C.blue2);
  addChip(slide, slideNo, "HOT PATH", 120, 606, 108, 28, C.blue, C.navy, C.blue);
  addChip(slide, slideNo, "COLD PATH", 542, 606, 110, 28, C.blue3, C.navy, C.blue3);
  addChip(slide, slideNo, "ANALYTICS", 956, 606, 116, 28, C.blue2, C.navy, C.blue2);
}

async function slideGoals(presentation) {
  const slideNo = 4;
  const slide = presentation.slides.add();
  addBackground(slide);
  addHeader(slide, slideNo, SLIDES[3].kicker, SLIDES[3].title, SLIDES[3].subtitle);
  addPanel(slide, slideNo, 60, 278, 320, 334, C.blue, C.panel2, C.line);
  addText(slide, slideNo, "Цель", 86, 304, 120, 24, {
    size: 22,
    color: C.blue,
    bold: true,
    face: F.title,
    role: "goal label",
  });
  addText(slide, slideNo, "Построить законченную Big Data-систему, которая одновременно показывает live-результат, сохраняет историю и даёт основу для аналитики и ML.", 86, 344, 250, 230, {
    size: 18,
    color: C.text,
    face: F.body,
    role: "goal body",
  });
  addPanel(slide, slideNo, 410, 278, 810, 334, C.blue3, C.panel2, C.line);
  addText(slide, slideNo, "Задачи", 436, 304, 180, 24, {
    size: 22,
    color: C.blue3,
    bold: true,
    face: F.title,
    role: "tasks label",
  });
  addBulletList(
    slide,
    slideNo,
    436,
    350,
    720,
    [
      "Подключить ingest к двум источникам видео и публиковать кадры в Redis и Kafka.",
      "Выполнить инференс YOLOv11s-seg, собрать маски, площади, confidence и метрики.",
      "Сохранить поток в HDFS/Parquet, затем перенести его в Hive-таблицы.",
      "Показать BI, историю, ML-эксперимент и масштабируемость Spark-конвейера.",
    ],
    C.blue3,
  );
}

async function slideFeatures(presentation) {
  const slideNo = 5;
  const slide = presentation.slides.add();
  addBackground(slide);
  addHeader(slide, slideNo, SLIDES[4].kicker, SLIDES[4].title, SLIDES[4].subtitle);
  const cards = [
    ["Live-сегментация", "YOLOv11s-seg строит маски льда и судна на каждом кадре."],
    ["Оценка льда", "Считаются `ice_conc`, `ice_severity`, площадь классов и наличие судна."],
    ["Мониторинг состояния", "API и Streamlit показывают состояние очередей, сервисов и логов."],
    ["История и BI", "Данные уходят в HDFS, Hive, Superset и Jupyter для отчётности."],
  ];
  const accents = [C.blue, C.blue3, C.blue2, C.blue];
  cards.forEach((card, idx) => {
    const col = idx % 2;
    const row = Math.floor(idx / 2);
    addCard(slide, slideNo, 60 + col * 580, 278 + row * 160, 540, 140, card[0], card[1], accents[idx], { bodySize: 13 });
  });
}

async function slideAudience(presentation) {
  const slideNo = 6;
  const slide = presentation.slides.add();
  addBackground(slide);
  addHeader(slide, slideNo, SLIDES[5].kicker, SLIDES[5].title, SLIDES[5].subtitle);
  addCard(slide, slideNo, 60, 278, 350, 292, "Оператор", "Смотрит live-видео, маски, тревожные состояния и тренды по двум камерам.", C.blue);
  addCard(slide, slideNo, 465, 278, 350, 292, "Аналитик", "Берёт Hive-таблицы, строит отчёты в SQL, Superset и Jupyter.", C.blue3);
  addCard(slide, slideNo, 870, 278, 350, 292, "Руководитель проекта", "Сверяет KPI, устойчивость контура и готовность системы к расширению.", C.blue2);
}

async function slideScenario(presentation) {
  const slideNo = 7;
  const slide = presentation.slides.add();
  addBackground(slide);
  addHeader(slide, slideNo, SLIDES[6].kicker, SLIDES[6].title, SLIDES[6].subtitle);
  addTimeline(slide, slideNo, 60, 286, 198, [
    { title: "Открыть панель", body: "Пользователь запускает Streamlit и видит общую сводку." },
    { title: "Посмотреть live", body: "Оператор переключается на камеры и следит за масками." },
    { title: "Проверить статус", body: "Через API видны pipeline_status и system_status." },
    { title: "Сохранить историю", body: "Поток пишет Parquet, Hive и TimescaleDB." },
    { title: "Сделать отчёт", body: "Аналитик открывает Superset или Jupyter." },
  ]);
}

async function slideComparison(presentation) {
  const slideNo = 8;
  const slide = presentation.slides.add();
  addBackground(slide);
  addHeader(slide, slideNo, SLIDES[7].kicker, SLIDES[7].title, SLIDES[7].subtitle);
  addComparisonGrid(
    slide,
    slideNo,
    50,
    262,
    236,
    64,
    ["Характеристика", "Ice Monitor", "RTSP viewer", "BI only", "Manual"],
    [
      ["Live сегментация", "✓", "✓", "△", "✗"],
      ["История и Hive", "✓", "✗", "✓", "✗"],
      ["Масштабирование", "✓", "△", "△", "✗"],
      ["ML и аналитика", "✓", "✗", "△", "✗"],
      ["Устойчивость", "✓", "△", "△", "✗"],
    ],
  );
  addText(slide, slideNo, "Легенда: ✓ полностью закрыто, △ частично, ✗ отсутствует.", 60, 648, 560, 16, {
    size: 10,
    color: C.faint,
    face: F.body,
    role: "footer",
  });
}

async function slideSoftware(presentation) {
  const slideNo = 9;
  const slide = presentation.slides.add();
  addBackground(slide);
  addHeader(slide, slideNo, SLIDES[8].kicker, SLIDES[8].title, SLIDES[8].subtitle);
  const cards = [
    ["Docker Compose", "Поднимает весь стек в одном файле и делает проект воспроизводимым."],
    ["Redis + Kafka", "Redis держит горячий поток, Kafka буферизует поток для batch-контуров."],
    ["FastAPI + Streamlit", "API обслуживает поток и статус, Streamlit даёт операторский UI."],
    ["YOLOv11s-seg", "Нужна маска, а не просто box-детекция: считаются площади и доли льда."],
    ["Spark + HDFS + Hive", "Классический big data слой для Parquet, ETL и SQL-аналитики."],
    ["TimescaleDB + Superset", "Горячие метрики и BI-дашборды для истории и контроля."],
  ];
  const colors = [C.blue, C.blue3, C.blue2, C.blue, C.blue3, C.blue2];
  cards.forEach((card, idx) => {
    const x = 60 + (idx % 3) * 390;
    const y = 278 + Math.floor(idx / 3) * 170;
    addCard(slide, slideNo, x, y, 360, 146, card[0], card[1], colors[idx], { bodySize: 12, titleSize: 15 });
  });
}

async function slideArchitecture(presentation) {
  const slideNo = 10;
  const slide = presentation.slides.add();
  addBackground(slide);
  addHeader(slide, slideNo, SLIDES[9].kicker, SLIDES[9].title, SLIDES[9].subtitle);
  addPanel(slide, slideNo, 60, 260, 1160, 362, C.blue, C.panel2, C.line);
  addText(slide, slideNo, "Hot path", 88, 286, 120, 20, {
    size: 18,
    color: C.blue,
    bold: true,
    face: F.mono,
    role: "arch hot label",
  });
  addText(slide, slideNo, "Cold path", 88, 446, 120, 20, {
    size: 18,
    color: C.blue3,
    bold: true,
    face: F.mono,
    role: "arch cold label",
  });
  const hot = buildArchitectureText();
  hot.forEach((item, idx) => {
    const x = 190 + idx * 190;
    addCard(slide, slideNo, x, 274, 160, 92, item[0], item[1], idx % 2 === 0 ? C.blue : C.blue2, { titleSize: 13, bodySize: 10 });
    if (idx < hot.length - 1) addChip(slide, slideNo, "→", x + 166, 301, 18, 24, C.blue4, C.panel, C.blue);
  });
  const cold = [
    ["Kafka", "ice.frames.*, ice.detections"],
    ["Spark Streaming", "Kafka → HDFS"],
    ["HDFS / Parquet", "сырой слой"],
    ["Spark ETL", "очистка и агрегации"],
    ["Hive", "таблицы и SQL"],
  ];
  cold.forEach((item, idx) => {
    const x = 190 + idx * 190;
    addCard(slide, slideNo, x, 434, 160, 92, item[0], item[1], idx % 2 === 0 ? C.blue3 : C.blue2, { titleSize: 13, bodySize: 10 });
    if (idx < cold.length - 1) addChip(slide, slideNo, "→", x + 166, 461, 18, 24, C.blue4, C.panel, C.blue3);
  });
}

async function slideData(presentation) {
  const slideNo = 11;
  const slide = presentation.slides.add();
  addBackground(slide);
  addHeader(slide, slideNo, SLIDES[10].kicker, SLIDES[10].title, SLIDES[10].subtitle);
  addMetricCard(slide, slideNo, 60, 274, 250, 116, "2", "ИСТОЧНИКА ВИДЕО", "cam_a.mp4 и cam_b.mp4, поток непрерывный и многопоточный.", C.blue);
  addMetricCard(slide, slideNo, 330, 274, 250, 116, "4", "КЛАССА ОБЪЕКТОВ", "vessel, ice_field, broken_ice, slush_ice.", C.blue3);
  addMetricCard(slide, slideNo, 600, 274, 250, 116, "5+", "СЛОЁВ ХРАНЕНИЯ", "Redis, Kafka, HDFS, Hive и TimescaleDB.", C.blue2);
  addMetricCard(slide, slideNo, 870, 274, 350, 116, "24/7", "ПОТОК СОБЫТИЙ", "Каждый кадр порождает маски, метрики и служебные записи.", C.blue);
  addPanel(slide, slideNo, 60, 418, 520, 210, C.blue, C.panel2, C.line);
  addText(slide, slideNo, "Структура данных", 84, 442, 220, 22, {
    size: 18,
    color: C.text,
    bold: true,
    face: F.title,
    role: "data structure title",
  });
  addBulletList(
    slide,
    slideNo,
    84,
    480,
    460,
    [
      "`ts`, `cam_id`, `frame_seq` и `source_ts` для временной привязки.",
      "`class_name`, `confidence`, `bbox_*`, `area_pixels` для детекций.",
      "`ice_conc`, `ice_severity`, `pixels_json` для аналитики и BI.",
      "`processed_ts` и `inference_ms` для измерения конвейера.",
    ],
    C.blue,
  );
  addPanel(slide, slideNo, 600, 418, 620, 210, C.blue3, C.panel2, C.line);
  addText(slide, slideNo, "Почему это big data", 624, 442, 240, 22, {
    size: 18,
    color: C.text,
    bold: true,
    face: F.title,
    role: "why big title",
  });
  addBulletList(
    slide,
    slideNo,
    624,
    480,
    560,
    [
      "Поток непрерывен, а данные хранятся сразу в нескольких слоях.",
      "Две камеры удваивают объём событий и ускоряют рост истории.",
      "Один кадр даёт не один факт, а набор масок, площадей и временных метрик.",
      "Batch-агрегации и BI требуют полного и чистого исторического слоя.",
    ],
    C.blue3,
  );
}

async function slideAlgorithms(presentation) {
  const slideNo = 12;
  const slide = presentation.slides.add();
  addBackground(slide);
  addHeader(slide, slideNo, SLIDES[11].kicker, SLIDES[11].title, SLIDES[11].subtitle);
  const cats = ["0.1M", "0.25M", "0.5M", "1.0M"];
  await addLineChart(slide, slideNo, 60, 286, 560, 300, "Время Spark ETL при росте объёма", cats, [
    { name: "1 local threads", values: [0.338936, 0.491122, 0.580761, 1.438394], color: C.blue },
    { name: "2 local threads", values: [0.362736, 0.337544, 0.539648, 0.600714], color: C.blue3 },
  ]);
  await addLineChart(slide, slideNo, 650, 286, 570, 300, "Speedup относительно базовой конфигурации", cats, [
    { name: "2 local threads", values: [0.94, 1.46, 1.08, 2.39], color: C.blue2 },
  ]);
  addMetricCard(slide, slideNo, 60, 612, 240, 72, "R² 0.9419", "1 thread", "Линейная модель хорошо описывает рост времени.", C.blue);
  addMetricCard(slide, slideNo, 318, 612, 240, 72, "R² 0.8285", "2 threads", "Разброс выше из-за планировщика и накладных расходов.", C.blue3);
  addMetricCard(slide, slideNo, 576, 612, 280, 72, "2.39x", "УСКОРЕНИЕ ПРИ 1M", "Параллелизм окупается на большом объёме.", C.blue2);
  addMetricCard(slide, slideNo, 874, 612, 346, 72, "O(n)", "АСИМПТОТИКА", "Время обработки растёт почти линейно, значит конвейер масштабируем.", C.blue);
}

async function slideDashboard(presentation) {
  const slideNo = 13;
  const slide = presentation.slides.add();
  addBackground(slide);
  addHeader(slide, slideNo, SLIDES[12].kicker, SLIDES[12].title, SLIDES[12].subtitle);
  addCard(slide, slideNo, 60, 276, 510, 116, "Hero-блок", "Показывает общий статус, суммарные KPI и заголовок операционной панели.", C.blue);
  addCard(slide, slideNo, 590, 276, 290, 116, "Карточки камер", "По каждой камере видны маски, риск, тренд и текущая сводка.", C.blue3);
  addCard(slide, slideNo, 900, 276, 320, 116, "Сервисы и логи", "Отдельный блок следит за API, Redis, Kafka, HDFS, Hive и Superset.", C.blue2);
  addPanel(slide, slideNo, 60, 418, 1160, 152, C.blue, C.panel2, C.line);
  addText(slide, slideNo, "Что делает дашборд полезным", 84, 444, 360, 22, {
    size: 18,
    color: C.text,
    bold: true,
    face: F.title,
    role: "dashboard title",
  });
  addBulletList(
    slide,
    slideNo,
    84,
    484,
    1030,
    [
      "Собирает live-видео, WebSocket-результаты, историю и сервисный статус в одном экране.",
      "Позволяет быстро понять, где проблема: в камере, модели, API или storage-слое.",
      "Скрывает сложность backend-стека и оставляет оператору короткий action path.",
    ],
    C.blue,
  );
}

async function slideDemo(presentation) {
  const slideNo = 14;
  const slide = presentation.slides.add();
  addBackground(slide);
  addHeader(slide, slideNo, SLIDES[13].kicker, SLIDES[13].title, SLIDES[13].subtitle);
  addTimeline(slide, slideNo, 60, 284, 200, [
    { title: "Open Streamlit", body: "Показать `/` и общий dashboard." },
    { title: "Check API", body: "Открыть `/docs`, `/system_status`, `/pipeline_status`." },
    { title: "Watch live", body: "Показать камеры, маски, опасность и логи." },
    { title: "Cold path", body: "Проверить Kafka, HDFS, Hive и ETL." },
    { title: "Analytics", body: "Superset, Jupyter и MLlib-эксперимент." },
  ]);
  addPanel(slide, slideNo, 60, 452, 560, 174, C.blue, C.panel2, C.line);
  addText(slide, slideNo, "Что запускать", 84, 476, 180, 20, {
    size: 18,
    color: C.text,
    bold: true,
    face: F.title,
    role: "demo launch title",
  });
  addText(
    slide,
    slideNo,
    "1. `make up`\n2. `http://localhost:8501`\n3. `http://localhost:8000/docs`\n4. `make check-hdfs`\n5. `make etl`\n6. `make mllib`\n7. `make analytics`",
    84,
    506,
    480,
    98,
    { size: 14, color: C.text, face: F.mono, role: "demo commands" },
  );
  addPanel(slide, slideNo, 650, 452, 570, 174, C.blue3, C.panel2, C.line);
  addText(slide, slideNo, "Что вставить на слайд", 674, 476, 240, 20, {
    size: 18,
    color: C.text,
    bold: true,
    face: F.title,
    role: "demo script title",
  });
  addText(
    slide,
    slideNo,
    "• Скрипт запуска hot-path benchmark\n• Путь: `scripts/run_hotpath_benchmark.ps1`\n• Или notebook: `notebooks/scaling_experiment.ipynb`\n• На защите показать, как данные проходят live и batch конвейеры",
    674,
    506,
    520,
    98,
    { size: 14, color: C.text, face: F.body, role: "demo script note" },
  );
}

async function slideResults(presentation) {
  const slideNo = 15;
  const slide = presentation.slides.add();
  addBackground(slide);
  addHeader(slide, slideNo, SLIDES[14].kicker, SLIDES[14].title, SLIDES[14].subtitle);
  addMetricCard(slide, slideNo, 60, 280, 250, 120, "75-78 ms", "MEDIAN LATENCY", "Hot path по текущему benchmark остаётся в районе десятков миллисекунд.", C.blue);
  addMetricCard(slide, slideNo, 330, 280, 250, 120, "1.6-2.0 ms", "INFERENCE", "Чистое время модели на одном кадре очень низкое относительно общего пути.", C.blue3);
  addMetricCard(slide, slideNo, 600, 280, 250, 120, "0-25%", "DROP RATE", "При росте числа камер виден честный компромисс между нагрузкой и стабильностью.", C.blue2);
  addMetricCard(slide, slideNo, 870, 280, 350, 120, "1.5 FPS", "THROUGHPUT", "Система выдерживает поток и даёт предсказуемую деградацию при нагрузке.", C.blue);
  addCard(slide, slideNo, 60, 430, 1160, 174, "Вывод", "Проект уже работает как полноценная платформа: live-контур отвечает за оператора, batch-контур сохраняет историю, а Spark-эксперимент подтверждает масштабируемость по объёму данных.", C.blue3, { bodySize: 16 });
}

async function slideRoadmap(presentation) {
  const slideNo = 16;
  const slide = presentation.slides.add();
  addBackground(slide);
  addHeader(slide, slideNo, SLIDES[15].kicker, SLIDES[15].title, SLIDES[15].subtitle);
  addCard(slide, slideNo, 60, 278, 260, 276, "Расширение потоков", "Подключить больше камер и сделать автоматическое масштабирование ingest/inference-слоя.", C.blue);
  addCard(slide, slideNo, 350, 278, 260, 276, "Лучшие модели", "Улучшить качество сегментации и добавить контроль качества по датасетам.", C.blue3);
  addCard(slide, slideNo, 640, 278, 260, 276, "Наблюдаемость", "Добавить более детальные метрики, алерты и дашборд инцидентов.", C.blue2);
  addCard(slide, slideNo, 930, 278, 290, 276, "Масштабирование хранения", "Разнести данные по партициям, расширить витрины и поднять долговременное хранение.", C.blue);
}

async function slideContacts(presentation) {
  const slideNo = 17;
  const slide = presentation.slides.add();
  addBackground(slide);
  addHeader(slide, slideNo, SLIDES[16].kicker, SLIDES[16].title, SLIDES[16].subtitle);
  addPanel(slide, slideNo, 60, 260, 360, 360, C.blue, C.panel2, C.line);
  addText(slide, slideNo, "QR-код GitHub", 86, 286, 220, 24, {
    size: 18,
    color: C.text,
    bold: true,
    face: F.title,
    role: "qr title",
  });
  if (await exists(QR_PATH)) {
    const image = slide.images.add({ blob: await readImageBlob(QR_PATH), fit: "contain", alt: "GitHub QR code" });
    image.position = { left: 90, top: 330, width: 220, height: 220 };
  } else {
    addShape(slide, "roundRect", 90, 330, 220, 220, C.blue4, C.line, 1, { slideNo, role: "qr placeholder" });
    addText(slide, slideNo, "QR", 90, 374, 220, 110, {
      size: 64,
      color: C.text,
      bold: true,
      face: F.title,
      align: "center",
      valign: "middle",
      role: "qr placeholder text",
    });
    addText(slide, slideNo, "REPLACE_WITH_GITHUB_URL", 94, 500, 212, 30, {
      size: 10,
      color: C.text,
      face: F.mono,
      align: "center",
      role: "qr placeholder note",
    });
  }
  addPanel(slide, slideNo, 460, 260, 760, 360, C.blue3, C.panel2, C.line);
  addText(slide, slideNo, "Контакты", 486, 286, 180, 24, {
    size: 18,
    color: C.text,
    bold: true,
    face: F.title,
    role: "contacts title",
  });
  addCard(slide, slideNo, 486, 330, 690, 96, "GitHub", "https://github.com/your-repository", C.blue);
  addCard(slide, slideNo, 486, 438, 690, 96, "Автор", "Имя, группа, e-mail или Telegram можно вставить в этот блок перед сдачей.", C.blue3);
  addCard(slide, slideNo, 486, 546, 690, 60, "Подпись", "Ice Monitor • Big Data • live + batch", C.blue2, { bodySize: 13 });
}

async function createNarrativePlan() {
  const content = `# Narrative Plan — Ice Monitor

## Audience
- Курс/комиссия по Big Data и прикладной аналитике.
- Акцент на архитектуре, реальных данных, масштабируемости и демонстрации живого контура.

## Objective
- Показать законченную платформу мониторинга ледовой обстановки: live-обработка видео, batch-аналитика, хранение, BI и ML.
- Доказать, что конвейер работает на больших данных и растёт предсказуемо.

## Slide List
1. Титульный слайд
2. Проблема и актуальность
3. Решение
4. Цель и задачи проекта
5. Основные функции
6. Целевая аудитория
7. Сценарий использования
8. Сравнение с аналогами
9. Выбор программных средств и назначение
10. Архитектура проекта
11. Сведения о данных
12. Алгоритмы внутри конвейера и масштабирование
13. Разработка и назначение дашборда
14. Демонстрация
15. Результаты и выводы
16. Планы по развитию и масштабированию
17. QR-код и контакты
`;
  await fs.writeFile(PLAN_PATH, content, "utf8");
}

async function buildDeck() {
  await ensureDirs();
  await createNarrativePlan();
  const presentation = Presentation.create({ slideSize: { width: W, height: H } });
  for (let idx = 0; idx < SLIDES.length; idx += 1) {
    const kind = SLIDES[idx].kind;
    if (idx === 0) await slideCover(presentation);
    else if (kind === "problem") await slideProblem(presentation);
    else if (kind === "solution") await slideSolution(presentation);
    else if (kind === "goals") await slideGoals(presentation);
    else if (kind === "features") await slideFeatures(presentation);
    else if (kind === "audience") await slideAudience(presentation);
    else if (kind === "scenario") await slideScenario(presentation);
    else if (kind === "comparison") await slideComparison(presentation);
    else if (kind === "software") await slideSoftware(presentation);
    else if (kind === "architecture") await slideArchitecture(presentation);
    else if (kind === "data") await slideData(presentation);
    else if (kind === "algorithms") await slideAlgorithms(presentation);
    else if (kind === "dashboard") await slideDashboard(presentation);
    else if (kind === "demo") await slideDemo(presentation);
    else if (kind === "results") await slideResults(presentation);
    else if (kind === "roadmap") await slideRoadmap(presentation);
    else if (kind === "contacts") await slideContacts(presentation);
  }
  return presentation;
}

async function saveBlob(blob, filePath) {
  const bytes = new Uint8Array(await blob.arrayBuffer());
  await fs.writeFile(filePath, bytes);
}

async function writeInspect(presentation) {
  const records = [
    { kind: "deck", id: "samsung", slideCount: presentation.slides.count, slideSize: { width: W, height: H } },
    ...INSPECT,
  ];
  await fs.writeFile(INSPECT_PATH, records.map((r) => JSON.stringify(r)).join("\n") + "\n", "utf8");
}

async function currentLoopCount() {
  const logPath = path.join(VERIFICATION_DIR, "render_verify_loops.ndjson");
  if (!(await exists(logPath))) return 0;
  const lines = (await fs.readFile(logPath, "utf8")).split(/\r?\n/).filter(Boolean);
  return lines.length;
}

async function exportAndVerify(presentation) {
  await writeInspect(presentation);
  const loopNo = 1;

  const previewPaths = [];
  for (let i = 0; i < presentation.slides.items.length; i += 1) {
    const slide = presentation.slides.items[i];
    const blob = await presentation.export({ slide, format: "png", scale: 1 });
    const previewPath = path.join(PREVIEW_DIR, `slide-${String(i + 1).padStart(2, "0")}.png`);
    await saveBlob(blob, previewPath);
    previewPaths.push(previewPath);
  }

  const pptxBlob = await PresentationFile.exportPptx(presentation);
  await pptxBlob.save(TARGET_PPTX);

  const logPath = path.join(VERIFICATION_DIR, "render_verify_loops.ndjson");
  const record = {
    kind: "render_verify_loop",
    deckId: "samsung",
    loop: loopNo,
    maxLoops: MAX_RENDER_VERIFY_LOOPS,
    timestamp: new Date().toISOString(),
    slideCount: presentation.slides.count,
    previewCount: previewPaths.length,
    previewDir: PREVIEW_DIR,
    inspectPath: INSPECT_PATH,
    pptxPath: TARGET_PPTX,
  };
  await fs.appendFile(logPath, JSON.stringify(record) + "\n", "utf8");
  return TARGET_PPTX;
}

try {
  const presentation = await buildDeck();
  const pptxPath = await exportAndVerify(presentation);
  console.log(pptxPath);
  process.exit(0);
} catch (error) {
  console.error(error);
  process.exit(1);
}
