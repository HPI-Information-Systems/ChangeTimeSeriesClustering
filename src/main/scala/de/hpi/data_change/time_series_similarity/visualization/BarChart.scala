package de.hpi.data_change.time_series_similarity.visualization

import java.awt.Dimension

import org.jfree.chart.{ChartFactory, ChartPanel, JFreeChart}
import org.jfree.chart.plot.PlotOrientation
import org.jfree.data.category.{CategoryDataset, DefaultCategoryDataset}
import org.jfree.ui.{ApplicationFrame, RefineryUtilities}
import org.jfree.chart.labels.ItemLabelAnchor
import org.jfree.chart.labels.ItemLabelPosition
import org.jfree.chart.labels.StandardCategoryItemLabelGenerator
import org.jfree.ui.TextAnchor
import org.jfree.chart.plot.CategoryPlot
import org.jfree.chart.renderer.category.CategoryItemRenderer


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

  val renderer: CategoryItemRenderer = chart.getPlot.asInstanceOf[CategoryPlot].getRenderer
  //val labelGen = new StandardCategoryItemLabelGenerator()
  val labelGen = new MyCategoryItemLabelGen()
  renderer.setBaseItemLabelGenerator(labelGen)
  renderer.setBaseItemLabelsVisible(true)
  val position = new ItemLabelPosition(ItemLabelAnchor.OUTSIDE12, TextAnchor.TOP_CENTER)
  renderer.setBasePositiveItemLabelPosition(position)
  val chartPanel = new ChartPanel(chart)
  //chartPanel.setPreferredSize(java.awt.Toolkit.getDefaultToolkit.getScreenSize)
  chartPanel.setPreferredSize(new Dimension(700,700))
  setContentPane(chartPanel)

  class MyCategoryItemLabelGen() extends StandardCategoryItemLabelGenerator{

    @Override override def generateLabel(dataset: CategoryDataset, row: Int, column: Int): String = {
      "%.2f".format(dataset.getValue(row,column)) + " (" + dataset.getRowKey(row).toString.replaceAll("_","\r\n") + ")"
    }
  }

}

