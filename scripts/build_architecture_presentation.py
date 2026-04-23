from __future__ import annotations

from pathlib import Path

import pythoncom
import win32com.client as win32


SLIDE_W = 960
SLIDE_H = 540
PP_LAYOUT_BLANK = 12
PP_SAVE_AS_PPTX = 24
MSO_SHAPE_RECTANGLE = 1
MSO_SHAPE_ROUNDED_RECTANGLE = 5
MSO_TEXT_ORIENTATION_HORIZONTAL = 1


def rgb(r: int, g: int, b: int) -> int:
    return r + (g << 8) + (b << 16)


def set_text(shape, text: str, size: int, color: int, bold: bool = False) -> None:
    frame = shape.TextFrame
    frame.WordWrap = True
    frame.TextRange.Text = text
    frame.TextRange.Font.Name = "Segoe UI"
    frame.TextRange.Font.Size = size
    frame.TextRange.Font.Color.RGB = color
    frame.TextRange.Font.Bold = -1 if bold else 0
    frame.TextRange.ParagraphFormat.Alignment = 2


def add_box(slide, left, top, width, height, title, subtitle, fill_rgb, line_rgb, title_size=15, sub_size=9):
    shp = slide.Shapes.AddShape(MSO_SHAPE_ROUNDED_RECTANGLE, left, top, width, height)
    shp.Fill.Solid()
    shp.Fill.ForeColor.RGB = fill_rgb
    shp.Fill.Transparency = 0.02
    shp.Line.ForeColor.RGB = line_rgb
    shp.Line.Weight = 1.4
    set_text(shp, f"{title}\n{subtitle}", title_size, rgb(235, 243, 255), bold=True)
    try:
        tr = shp.TextFrame.TextRange
        tr.Characters(1, len(title)).Font.Size = title_size
        tr.Characters(1, len(title)).Font.Bold = -1
        tr.Characters(len(title) + 2, len(subtitle)).Font.Size = sub_size
        tr.Characters(len(title) + 2, len(subtitle)).Font.Bold = 0
        tr.Characters(len(title) + 2, len(subtitle)).Font.Color.RGB = rgb(174, 191, 214)
    except Exception:
        pass
    return shp


def add_text(slide, left, top, width, height, text, size=14, color=rgb(235, 243, 255), bold=False):
    shp = slide.Shapes.AddTextbox(MSO_TEXT_ORIENTATION_HORIZONTAL, left, top, width, height)
    set_text(shp, text, size, color, bold=bold)
    return shp


def add_line(slide, x1, y1, x2, y2, line_rgb, weight=2.0, arrow=True, dash=False):
    line = slide.Shapes.AddLine(x1, y1, x2, y2)
    line.Line.ForeColor.RGB = line_rgb
    line.Line.Weight = weight
    if dash:
        line.Line.DashStyle = 4
    if arrow:
        try:
            line.Line.EndArrowheadStyle = 3
        except Exception:
            pass
    return line


def build_presentation(out_path: Path) -> None:
    pythoncom.CoInitialize()
    app = None
    pres = None
    try:
        app = win32.DispatchEx("PowerPoint.Application")
        app.DisplayAlerts = 0
        pres = app.Presentations.Add()
        pres.PageSetup.SlideWidth = SLIDE_W
        pres.PageSetup.SlideHeight = SLIDE_H

        slide = pres.Slides.Add(1, PP_LAYOUT_BLANK)
        slide.Background.Fill.Solid()
        slide.Background.Fill.ForeColor.RGB = rgb(6, 13, 24)

        # decorative background glows
        for left, top, size, color, trans in [
            (700, 18, 240, rgb(80, 190, 255), 0.84),
            (18, 392, 190, rgb(100, 255, 198), 0.90),
            (820, 385, 120, rgb(255, 198, 108), 0.88),
        ]:
            glow = slide.Shapes.AddShape(MSO_SHAPE_RECTANGLE, left, top, size, size)
            glow.Fill.Solid()
            glow.Fill.ForeColor.RGB = color
            glow.Fill.Transparency = trans
            glow.Line.Visible = 0

        add_text(slide, 32, 16, 620, 30, "Схема работы проекта Ice Monitor", size=25, bold=True)
        add_text(
            slide,
            32,
            46,
            720,
            30,
            "Онлайн-контур даёт live-видео и статусы, а пакетный контур сохраняет историю, аналитику и ML.",
            size=11,
            color=rgb(160, 178, 203),
        )

        # small tags
        for x, label, color, w in [
            (770, "docker compose", rgb(19, 37, 58), 150),
            (770, "kafka-init", rgb(41, 30, 8), 110),
            (885, "hdfs-init", rgb(41, 30, 8), 95),
        ]:
            tag = slide.Shapes.AddShape(MSO_SHAPE_ROUNDED_RECTANGLE, x, 40 if label != "docker compose" else 12, w, 22)
            tag.Fill.Solid()
            tag.Fill.ForeColor.RGB = color
            tag.Line.ForeColor.RGB = rgb(95, 131, 168)
            tag.Line.Weight = 1.0
            add_text(slide, x + 6, (40 if label != "docker compose" else 12) + 2, w - 12, 18, label, size=8, bold=True, color=rgb(235, 245, 255))

        # panels
        hot_x, hot_y, hot_w, hot_h = 24, 90, 446, 360
        cold_x, cold_y, cold_w, cold_h = 490, 90, 446, 360

        for x, y, w, h, title, subtitle, accent in [
            (hot_x, hot_y, hot_w, hot_h, "Онлайн-контур", "Поток от видео до live-интерфейса", rgb(84, 198, 255)),
            (cold_x, cold_y, cold_w, cold_h, "Пакетный контур", "История, Hive, BI и ML", rgb(255, 195, 108)),
        ]:
            panel = slide.Shapes.AddShape(MSO_SHAPE_ROUNDED_RECTANGLE, x, y, w, h)
            panel.Fill.Solid()
            panel.Fill.ForeColor.RGB = rgb(12, 23, 39) if accent == rgb(84, 198, 255) else rgb(24, 19, 10)
            panel.Fill.Transparency = 0.02
            panel.Line.ForeColor.RGB = accent
            panel.Line.Transparency = 0.72
            panel.Line.Weight = 1.5
            header = slide.Shapes.AddShape(MSO_SHAPE_RECTANGLE, x, y, w, 42)
            header.Fill.Solid()
            header.Fill.ForeColor.RGB = rgb(15, 28, 48) if accent == rgb(84, 198, 255) else rgb(35, 26, 12)
            header.Line.Visible = 0
            add_text(slide, x + 16, y + 8, w - 30, 18, title, size=15, bold=True)
            add_text(slide, x + 16, y + 24, w - 30, 14, subtitle, size=8, color=rgb(152, 169, 194))

        # hot path
        hx = 46
        hw = 252
        hnote_x = 314
        hh = 42
        y_positions = [146, 200, 254, 308, 362]
        hot_boxes = [
            ("Видеофайлы камер", "cam_a / cam_b"),
            ("ingest", "кадры в Redis и Kafka"),
            ("Redis Streams", "буфер кадров"),
            ("inference / YOLOv11s-seg", "маски и события"),
            ("FastAPI + Streamlit", "live UI / WS / status"),
        ]

        hot_shapes = []
        for (title, subtitle), y in zip(hot_boxes, y_positions):
            box = add_box(
                slide,
                hx,
                y,
                hw,
                hh,
                title,
                subtitle,
                fill_rgb=rgb(16, 29, 49),
                line_rgb=rgb(95, 195, 255),
                title_size=13 if len(title) > 18 else 14,
                sub_size=8,
            )
            hot_shapes.append(box)

        db = add_box(
            slide,
            hnote_x,
            304,
            122,
            86,
            "TimescaleDB",
            "события и статусы",
            fill_rgb=rgb(8, 18, 30),
            line_rgb=rgb(122, 220, 255),
            title_size=12,
            sub_size=8,
        )

        for prev, nxt in zip(hot_shapes, hot_shapes[1:]):
            add_line(
                slide,
                prev.Left + prev.Width / 2,
                prev.Top + prev.Height,
                nxt.Left + nxt.Width / 2,
                nxt.Top,
                rgb(95, 195, 255),
            )

        add_line(
            slide,
            hot_shapes[1].Left + hot_shapes[1].Width,
            hot_shapes[1].Top + hot_shapes[1].Height / 2,
            cold_x + 24,
            cold_y + 112,
            rgb(95, 195, 255),
            weight=2.1,
            dash=True,
        )
        add_text(slide, 426, 232, 70, 20, "Kafka", size=9, color=rgb(113, 202, 255), bold=True)

        add_line(
            slide,
            hot_shapes[-1].Left + hot_shapes[-1].Width,
            hot_shapes[-1].Top + hot_shapes[-1].Height / 2,
            db.Left,
            db.Top + db.Height / 2,
            rgb(122, 220, 255),
            weight=2.0,
        )

        # cold path
        cx = 512
        cw = 242
        ch = 42
        y_positions_cold = [146, 200, 254, 308, 362]
        cold_boxes = [
            ("Kafka topics", "ice.frames.*, ice.detections"),
            ("Spark Streaming", "kafka-to-hdfs"),
            ("HDFS / Parquet", "raw layer"),
            ("Spark ETL", "etl_to_hive"),
            ("Hive tables", "frames / detections / summary"),
        ]
        cold_shapes = []
        for (title, subtitle), y in zip(cold_boxes, y_positions_cold):
            box = add_box(
                slide,
                cx,
                y,
                cw,
                ch,
                title,
                subtitle,
                fill_rgb=rgb(28, 22, 12),
                line_rgb=rgb(255, 195, 108),
                title_size=13 if len(title) > 14 else 14,
                sub_size=8,
            )
            cold_shapes.append(box)

        for prev, nxt in zip(cold_shapes, cold_shapes[1:]):
            add_line(
                slide,
                prev.Left + prev.Width / 2,
                prev.Top + prev.Height,
                nxt.Left + nxt.Width / 2,
                nxt.Top,
                rgb(255, 195, 108),
            )

        branch_y = 438
        branch_w = 120
        branch_gap = 12
        branch_xs = [512, 512 + branch_w + branch_gap, 512 + 2 * (branch_w + branch_gap)]
        branch_labels = [("Superset", "BI dashboards"), ("Jupyter", "notebooks / report"), ("MLlib", "scaling experiment")]
        branch_boxes = []
        for (title, subtitle), x in zip(branch_labels, branch_xs):
            branch = add_box(
                slide,
                x,
                branch_y,
                branch_w,
                34,
                title,
                subtitle,
                fill_rgb=rgb(24, 18, 8),
                line_rgb=rgb(255, 205, 123),
                title_size=10,
                sub_size=7,
            )
            branch_boxes.append(branch)

        for branch in branch_boxes:
            add_line(
                slide,
                cold_shapes[-1].Left + cold_shapes[-1].Width / 2,
                cold_shapes[-1].Top + cold_shapes[-1].Height,
                branch.Left + branch.Width / 2,
                branch.Top,
                rgb(255, 195, 108),
                weight=1.8,
            )

        # bottom band
        band = slide.Shapes.AddShape(MSO_SHAPE_ROUNDED_RECTANGLE, 24, 492, 912, 34)
        band.Fill.Solid()
        band.Fill.ForeColor.RGB = rgb(11, 19, 32)
        band.Line.ForeColor.RGB = rgb(96, 120, 150)
        band.Line.Transparency = 0.82
        add_text(
            slide,
            42,
            499,
            820,
            16,
            "Главная идея: live-контур отвечает за оперативную реакцию, а batch-контур превращает поток в историю, аналитику и ML.",
            size=10,
            color=rgb(207, 220, 236),
            bold=False,
        )
        for x, label, color, w in [
            (782, "REAL-TIME", rgb(16, 37, 58), 88),
            (878, "BATCH", rgb(42, 30, 9), 58),
        ]:
            pill = slide.Shapes.AddShape(MSO_SHAPE_ROUNDED_RECTANGLE, x, 499, w, 18)
            pill.Fill.Solid()
            pill.Fill.ForeColor.RGB = color
            pill.Line.Visible = 0
            add_text(slide, x, 501, w, 10, label, size=7, color=rgb(235, 245, 255), bold=True)

        pres.SaveAs(str(out_path), PP_SAVE_AS_PPTX)
    finally:
        if pres is not None:
            pres.Close()
        if app is not None:
            app.Quit()
        pythoncom.CoUninitialize()


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    out_path = root / "presentation" / "ice_monitor_architecture.pptx"
    build_presentation(out_path)
    print(out_path)


if __name__ == "__main__":
    main()
