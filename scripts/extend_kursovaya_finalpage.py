from __future__ import annotations

from pathlib import Path

from expand_kursovaya_docx import (
    build_paragraph,
    insert_before,
    load_document_xml,
    write_docx,
)


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    docx_path = repo_root / "kursovaya" / "Курсовая ГО.docx"
    if not docx_path.exists():
        raise FileNotFoundError(docx_path)

    root, entries, root_open_tag = load_document_xml(docx_path)
    body = root.find("{http://schemas.openxmlformats.org/wordprocessingml/2006/main}body")
    if body is None:
        raise RuntimeError("word/document.xml has no body")

    extra = [
        build_paragraph("5.14. Финальный вывод по готовности проекта", "Heading2"),
        build_paragraph(
            "Суммарно проект достиг той степени завершённости, когда его можно "
            "использовать как полноценный учебный пример современного интеллектуального "
            "конвейера обработки данных. В нём есть логика построения модели, "
            "механизмы передачи и хранения данных, аналитический слой и визуальный "
            "интерфейс. Такая полнота делает результаты работы не только осмысленными, "
            "но и демонстрационно убедительными."
        ),
        build_paragraph(
            "Именно поэтому Ice Monitor можно считать не просто курсовой работой, а "
            "компактной исследовательско-инженерной платформой, на которой наглядно "
            "видно, как глубокое обучение превращается в прикладной сервис. Для защиты "
            "и для дальнейшего изучения этого достаточно, чтобы показать целостность "
            "решения и его потенциал для развития."
        ),
    ]

    insert_before(body, "ЗАКЛЮЧЕНИЕ", extra)
    write_docx(docx_path, entries, root, root_open_tag)
    print(f"Updated: {docx_path}")


if __name__ == "__main__":
    main()
