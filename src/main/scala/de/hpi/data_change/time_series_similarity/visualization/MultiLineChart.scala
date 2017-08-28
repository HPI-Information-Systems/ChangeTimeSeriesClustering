package de.hpi.data_change.time_series_similarity.visualization

import java.awt.{Color, Dimension}

import org.jfree.chart.{ChartFactory, ChartPanel}
import org.jfree.chart.axis.NumberAxis
import org.jfree.chart.plot.PlotOrientation
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer
import org.jfree.data.xy.{XYDataset, XYSeriesCollection}
import org.jfree.ui.ApplicationFrame
import org.jfree.ui.RefineryUtilities

class MultiLineChart(title:String,collection: XYSeriesCollection) extends ApplicationFrame(title) {

  val chart = createChart(collection)
  val chartPanel = new ChartPanel(chart)
  chartPanel.setPreferredSize(new Dimension(500, 270))
  setContentPane(chartPanel)

  def draw() = {
    pack()
    RefineryUtilities.centerFrameOnScreen(this)
    setVisible(true)
  }

  private def createChart(dataset:XYDataset) = { // create the chart...
    val chart = ChartFactory.createXYLineChart("Line Chart Demo 6", // chart title
      "X", // x axis label
      "Y", // y axis label
      dataset, // data
      PlotOrientation.VERTICAL, true, // include legend
      true, // tooltips
      false) // urls)
    // NOW DO SOME OPTIONAL CUSTOMISATION OF THE CHART...
    chart.setBackgroundPaint(Color.white)
    //        final StandardLegend legend = (StandardLegend) chart.getLegend();
    //      legend.setDisplaySeriesShapes(true);
    // get a reference to the plot for further customisation...
    val plot = chart.getXYPlot
    plot.setBackgroundPaint(Color.lightGray)
    //    plot.setAxisOffset(new Spacer(Spacer.ABSOLUTE, 5.0, 5.0, 5.0, 5.0));
    plot.setDomainGridlinePaint(Color.white)
    plot.setRangeGridlinePaint(Color.white)
    val renderer = new XYLineAndShapeRenderer
    renderer.setSeriesLinesVisible(0, false)
    renderer.setSeriesShapesVisible(1, false)
    plot.setRenderer(renderer)
    // change the auto tick unit selection to integer units only...
    val rangeAxis = plot.getRangeAxis.asInstanceOf[NumberAxis]
    rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits)
    // OPTIONAL CUSTOMISATION COMPLETED.
    chart
  }
}
