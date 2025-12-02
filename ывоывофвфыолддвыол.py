import numpy as np
import matplotlib.pyplot as plt

# --- 1. Определение функций ---
def x(t):
    return 3*t**2 + 2*t + 2

def y(t):
    return 4*t**3 + 5*np.cos(np.pi * t / 4)

def vx(t):
    return 6*t + 2

def vy(t):
    return 12*t**2 - 5 * (np.pi/4) * np.sin(np.pi * t / 4)

def ax(t):
    return 6

def ay(t):
    return 24*t - 5 * (np.pi/4)**2 * np.cos(np.pi * t / 4)

# --- 2. Генерация данных для графиков ---
t_values = np.linspace(0, 4, 200) # Диапазон времени от 0 до 4 секунд для плавной траектории

x_coords = x(t_values)
y_coords = y(t_values)

vx_values = vx(t_values)
vy_values = vy(t_values)

ax_values = ax(t_values)
ay_values = ay(t_values)

# --- 3. Точка P(7, 7.536), соответствующая t=1 с ---
t_point = 1
Px = x(t_point)
Py = y(t_point)
Vx_at_P = vx(t_point)
Vy_at_P = vy(t_point)
Ax_at_P = ax(t_point)
Ay_at_P = ay(t_point)

# --- 4. Построение графиков ---

# График 1: Траектория движения с векторами скорости и ускорения в t=1с
plt.figure(figsize=(10, 8))
plt.plot(x_coords, y_coords, label='Траектория точки', color='blue', linewidth=2)

# Отметим ключевые точки
points_t_values = [0, 1, 2, 3]
for t_val in points_t_values:
    current_x = x(t_val)
    current_y = y(t_val)
    plt.plot(current_x, current_y, 'o', markersize=7, color='red', zorder=5)
    plt.text(current_x + 2, current_y + 5, f'P({current_x:.0f}, {current_y:.0f}) t={t_val}с', fontsize=9, color='red')

# Нарисуем векторы скорости и ускорения в точке P(7, 7.536)
# Масштабирование векторов для лучшей визуализации на графике
# (длины векторов на графике будут отличаться от их истинных значений для наглядности)
vector_display_scale = 0.5

# Вектор скорости
plt.arrow(Px, Py, Vx_at_P * vector_display_scale, Vy_at_P * vector_display_scale,
          color='green', width=0.8, head_width=3, head_length=5,
          length_includes_head=True, label=r'$\vec{v}$ в $t=1с$')
# Вектор ускорения
plt.arrow(Px, Py, Ax_at_P * vector_display_scale, Ay_at_P * vector_display_scale,
          color='red', width=0.8, head_width=3, head_length=5,
          length_includes_head=True, label=r'$\vec{a}$ в $t=1с$')

plt.xlabel('x (м)')
plt.ylabel('y (м)')
plt.title('Траектория движения точки с векторами скорости и ускорения')
plt.grid(True)
plt.legend()
plt.gca().set_aspect('auto') # Автоматическое соотношение сторон
plt.show()

# График 2: Компоненты вектора скорости как функции времени
plt.figure(figsize=(10, 6))
plt.plot(t_values, vx_values, label=r'$v_x(t) = 6t + 2$', color='purple', linewidth=2)
plt.plot(t_values, vy_values, label=r'$v_y(t) = 12t^2 - \frac{5\pi}{4}\sin(\frac{\pi t}{4})$', color='orange', linewidth=2)
plt.axvline(x=t_point, color='gray', linestyle='--', label=f'Момент $t={t_point}$с') # Линия для t=1с
plt.plot(t_point, Vx_at_P, 'o', markersize=7, color='purple', zorder=5, label=f'$v_x({t_point})$ = {Vx_at_P:.3f} м/с')
plt.plot(t_point, Vy_at_P, 'o', markersize=7, color='orange', zorder=5, label=f'$v_y({t_point})$ = {Vy_at_P:.3f} м/с')

plt.xlabel('Время $t$ (с)')
plt.ylabel('Скорость (м/с)')
plt.title('Компоненты вектора скорости')
plt.grid(True)
plt.legend()
plt.show()

# График 3: Компоненты вектора ускорения как функции времени
plt.figure(figsize=(10, 6))
plt.plot(t_values, ax_values, label=r'$a_x(t) = 6$', color='brown', linewidth=2)
plt.plot(t_values, ay_values, label=r'$a_y(t) = 24t - \frac{5\pi^2}{16}\cos(\frac{\pi t}{4})$', color='teal', linewidth=2)
plt.axvline(x=t_point, color='gray', linestyle='--', label=f'Момент $t={t_point}$с') # Линия для t=1с
plt.plot(t_point, Ax_at_P, 'o', markersize=7, color='brown', zorder=5, label=f'$a_x({t_point})$ = {Ax_at_P:.3f} м/с$^2$')
plt.plot(t_point, Ay_at_P, 'o', markersize=7, color='teal', zorder=5, label=f'$a_y({t_point})$ = {Ay_at_P:.3f} м/с$^2$')

plt.xlabel('Время $t$ (с)')
plt.ylabel('Ускорение (м/с$^2$)')
plt.title('Компоненты вектора ускорения')
plt.grid(True)
plt.legend()
plt.show()
