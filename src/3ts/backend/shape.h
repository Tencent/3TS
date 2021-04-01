#include <stdio.h>
#include <iostream>
#include <math.h>

float f(float x, float y, float z) {
  float a = x * x + 9.0f / 4.0f * y * y + z * z - 1;
  return a * a * a - x * x * z * z * z - 9.0f / 80.0f * y * y * z * z * z;
}

float h(float x, float z) {
  for (float y = 1.0f; y >= 0.0f; y -= 0.001f) {
    if (f(x, y, z) <= 0.0f) {
      return y;
    }
  }
  return 0.0f;
}

void draw() {
  for (float z = 1.5f; z > -1.5f; z -= 0.05f) {
    for (float x = -1.5f; x < 1.5f; x += 0.025f) {
      float v = f(x, 0.0f, z);
      if (v <= 0.0f) {
        float y0 = h(x, z);
        float ny = 0.01f;
        float nx = h(x + ny, z) - y0;
        float nz = h(x, z + ny) - y0;
        float nd = 1.0f / sqrtf(nx * nx + ny * ny + nz * nz);
        float d = (nx + ny - nz) * nd * 0.5f + 0.5f;
        putchar("*********"[(int)(d * 5.0f)]);
      } else {
        putchar(' ');
      }
    }
    putchar('\n');
  }
  std::cout << "Congratulations on finding the Easter egg! Here's a gift from us for you!" << std::endl;;
}
