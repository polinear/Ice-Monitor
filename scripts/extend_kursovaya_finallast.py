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
        build_paragraph("5.15. Краткое резюме по выполненной работе", "Heading2"),
        build_paragraph(
            "Выполненная работа демонстрирует полный путь от постановки прикладной задачи "
            "до построения законченной программной системы. Такой подход особенно важен "
            "для технической курсовой, потому что позволяет оценивать не только знания "
            "отдельных инструментов, но и умение связывать их в единую архитектуру."
        ),
        build_paragraph(
            "Для Ice Monitor это означает, что глубокое обучение, big data-инфраструктура "
            "и пользовательский интерфейс объединены в один осмысленный сценарий. "
            "Именно эта связность делает проект полезным как образец современного "
            "прикладного решения и как демонстрацию системного мышления."
        ),
        build_paragraph(
            "Тем самым работа достигает своей основной цели: она не только описывает "
            "технологии, но и показывает, как они функционируют вместе в реальном "
            "проекте. Для защиты и последующего использования этого достаточно, чтобы "
            "рассматривать Ice Monitor как завершённую и содержательную курсовую работу."
        ),
    ]

    insert_before(body, "ЗАКЛЮЧЕНИЕ", extra)
    write_docx(docx_path, entries, root, root_open_tag)
    print(f"Updated: {docx_path}")


if __name__ == "__main__":
    main()
