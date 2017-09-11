package de.hpi.data_change.time_series_similarity.visualization

import java.awt.{Color, Dimension, Paint}

import de.hpi.data_change.time_series_similarity.configuration.ClusteringConfig
import org.jfree.chart.{ChartFactory, ChartPanel}
import org.jfree.chart.axis.{LogAxis, NumberAxis}
import org.jfree.chart.plot.PlotOrientation
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer
import org.jfree.data.xy.{XYDataset, XYSeriesCollection}
import org.jfree.ui.ApplicationFrame
import org.jfree.ui.RefineryUtilities

class MultiLineChart(title:String, clusteringConfig: ClusteringConfig, collection: XYSeriesCollection, yAxisAsLog:Boolean = false) extends ApplicationFrame(title) {

  val chart = createChart(collection)
  val chartPanel = new ChartPanel(chart)
  chartPanel.setPreferredSize(new Dimension(900, 300))
  setContentPane(chartPanel)

  def draw() = {
    pack()
    RefineryUtilities.centerFrameOnScreen(this)
    setVisible(true)
  }

  def draw(x:Int,y:Int) = {
    pack()
    setLocation(x,y)
    setVisible(true)
  }

  def buildDetailedTitle(): String = {
    clusteringConfig.groupingKey.toString + ", " + clusteringConfig.granularity.toString + ", " + clusteringConfig.clusteringAlgorithmParameters("k")
  }


  private def createChart(dataset:XYSeriesCollection) = { // create the chart...
    val chart = ChartFactory.createXYLineChart(buildDetailedTitle(), // chart title
      "X", // x axis label
      "Y", // y axis label
      dataset, // data
      PlotOrientation.VERTICAL, true, // include legend
      true, // tooltipsa
      false) // urls)
    // NOW DO SOME OPTIONAL CUSTOMISATION OF THE CHART...
    if (yAxisAsLog){
      chart.setBackgroundPaint(Color.white)
      chart.getXYPlot.setRangeAxis(new LogAxis("Log Y"))
    }
    //        final StandardLegend legend = (StandardLegend) chart.getLegend();
    //      legend.setDisplaySeriesShapes(true);
    // get a reference to the plot for further customisation...
    val plot = chart.getXYPlot
    plot.setBackgroundPaint(Color.lightGray)
    //    plot.setAxisOffset(new Spacer(Spacer.ABSOLUTE, 5.0, 5.0, 5.0, 5.0));
    plot.setDomainGridlinePaint(Color.white)
    plot.setRangeGridlinePaint(Color.white)
    val renderer = new XYLineAndShapeRenderer

    ( 0 until dataset.getSeriesCount).foreach( i => {
      renderer.setSeriesLinesVisible(i, true)
      renderer.setSeriesShapesVisible(i, true)
    })
    plot.setRenderer(renderer)
    // change the auto tick unit selection to integer units only...
    //val rangeAxis = plot.getRangeAxis.asInstanceOf[NumberAxis]
    //rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits)
    // OPTIONAL CUSTOMISATION COMPLETED.
    chart
  }
}
