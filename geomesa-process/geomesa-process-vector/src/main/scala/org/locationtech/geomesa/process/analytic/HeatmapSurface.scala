/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.analytic

import org.locationtech.jts.geom.Envelope

class HeatmapSurface(kernelRadius: Int,
                     srcEnv: Envelope,
                     xSize: Int,
                     ySize: Int,
                     normalize: Boolean = true) {

  type Array2D = Array[Array[Float]]

  private val GAUSSIAN_APPROX_ITER = 4
  private val kernelRadiusGrid: Int = Math.max(kernelRadius, 0)
  private val gridTransform: GridTransform =
    new GridTransform(srcEnv, xSize, ySize, isClamped = false)
  private val grid: Array2D = {
    val xSizeExp: Int = xSize + 2 * kernelRadiusGrid
    val ySizeExp: Int = ySize + 2 * kernelRadiusGrid
    Array.ofDim[Float](xSizeExp, ySizeExp)
  }

  def addPoint(x: Double, y: Double, value: Double): Unit = {
    val gi: Int = gridTransform.i(x) + kernelRadiusGrid
    val gj: Int = gridTransform.j(y) + kernelRadiusGrid

    val outsideGrid: Boolean =
      gi < 0 || gi > grid.length || gj < 0 || gj > grid(0).length

    if (!outsideGrid) {
      grid(gi)(gj) += value.asInstanceOf[Float]
    }
  }

  def computeSurface(): Array2D = {
    computeHeatmap(grid, kernelRadiusGrid)
    extractGrid(grid, kernelRadiusGrid, kernelRadiusGrid, xSize, ySize)
  }

  def extractGrid(grid: Array2D,
                  xBase: Int,
                  yBase: Int,
                  xSize: Int,
                  ySize: Int): Array2D = {

    val gridExtract: Array2D = Array.ofDim[Float](xSize, ySize)

    for {
      i <- Seq.range(0, xSize)
      j <- Seq.range(0, ySize)
    } gridExtract(i)(j) = grid(xBase + i)(yBase + j)

    gridExtract
  }

  def computeHeatmap(grid: Array2D, kernelRadius: Int): Array2D = {
    val xSize = grid.length
    val ySize = grid(0).length

    val baseBoxKernelRadius = kernelRadius / GAUSSIAN_APPROX_ITER
    val radiusIncBreak = kernelRadius - baseBoxKernelRadius * GAUSSIAN_APPROX_ITER

    val grid2 = Array.ofDim[Float](ySize, xSize)
    for (count <- Seq.range(0, GAUSSIAN_APPROX_ITER)) {
      var boxKernelRadius = baseBoxKernelRadius
      if (count < radiusIncBreak) boxKernelRadius += 1
      boxBlur(boxKernelRadius, grid, grid2)
      boxBlur(boxKernelRadius, grid2, grid)
    }

    if (normalize) normalize(grid) else grid
  }

  def normalize(grid: Array2D): Array2D = {
    val normFactor = 1.0f / grid.flatten.max
    grid.map(_.map(_ * normFactor))
  }

  def kernelVal(kernelRadius: Int): Float = 1.0f / (2 * kernelRadius + 1)

  def boxBlur(kernelRadius: Int, input: Array2D, output: Array2D): Unit = {
    val width = input.length
    val height = input(0).length

    val kVal = kernelVal(kernelRadius)

    for (j <- Seq.range(0, height)) {

      var tot = 0.0

      for (i <- Seq.range(-kernelRadius, kernelRadius+1)) {
        if (i >= 0 && i < width)
          tot += input(i)(j) * kVal
      }

      output(j)(0) = tot.asInstanceOf[Float]

      for (i <- Seq.range(1, width)) {
        val iprev = i - 1 - kernelRadius
        if (iprev >= 0)
          tot -= input(iprev)(j) * kVal

        val inext = i + kernelRadius
        if (inext < width)
          tot += input(inext)(j) * kVal

        output(j)(i) = tot.asInstanceOf[Float]
      }
    }
  }
}
