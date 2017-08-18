package de.hpi.data_change.time_series_similarity.visualization

import java.awt.Dimension

import org.jfree.chart.{ChartFactory, ChartPanel, JFreeChart}
import org.jfree.chart.plot.PlotOrientation
import org.jfree.data.category.{CategoryDataset, DefaultCategoryDataset}
import org.jfree.ui.{ApplicationFrame, RefineryUtilities}

class BarChart(title: String, data: Seq[(String, String, Double)],relative:Boolean) extends ApplicationFrame(title){

  def createDataset(data: Seq[(String, String, Double)]):CategoryDataset = {
    val result = new DefaultCategoryDataset
    if(!relative) {
      data.foreach { case (innerCategory, outerCategory, value) => result.addValue(value, innerCategory, outerCategory) }
    } else{
      val sumsByCategory = data
        .groupBy( t => t._2)
        .map{case (outerCategory,list) => (outerCategory,list.map{case (_,_,value) => value}.sum)}
      data.foreach{ case (innerCategory, outerCategory, value) => result.addValue(value.toDouble / sumsByCategory.get(outerCategory).get, innerCategory, outerCategory) }
    }
    return result
  }

  def draw() = {
    pack()
    RefineryUtilities.centerFrameOnScreen(this)
    setVisible(true)
  }

  val dataset = createDataset(data)

  def createChart(): _root_.org.jfree.chart.JFreeChart = {
    val chart = ChartFactory.createStackedBarChart("Stacked Bar Chart Demo 4", // chart title
      "Category", // domain axis label
      "Value", // range axis label
      dataset, // data
      PlotOrientation.VERTICAL, // the plot orientation
      true, // legend
      true, // tooltips
      false) // urls)
    chart
  }

  val chart: JFreeChart = createChart()
  val chartPanel = new ChartPanel(chart)
  //chartPanel.setPreferredSize(java.awt.Toolkit.getDefaultToolkit.getScreenSize)
  chartPanel.setPreferredSize(new Dimension(700,700))
  setContentPane(chartPanel)

}

