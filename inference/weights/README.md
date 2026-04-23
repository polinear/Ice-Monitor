# Веса моделей

Сюда положи обученные модели YOLOv11s-seg:

- `cam_a.pt` — модель для первой камеры
- `cam_b.pt` — модель для второй камеры

Классы обеих моделей должны совпадать:
```
0: slush_ice
1: open_water
2: broken_ice
3: vessel
4: ice_field
5: background
```

## Для тестирования без обученной модели

Можно подложить стандартную `yolo11s-seg.pt` из ultralytics под оба имени —
она обучена на COCO (80 классов), результаты будут бессмысленные,
но пайплайн можно проверить:

```bash
pip install ultralytics
yolo download model=yolo11s-seg.pt
cp yolo11s-seg.pt cam_a.pt
cp yolo11s-seg.pt cam_b.pt
```

В этом случае **обнови список классов** в `inference/worker.py`, иначе
детекции будут фильтроваться.
