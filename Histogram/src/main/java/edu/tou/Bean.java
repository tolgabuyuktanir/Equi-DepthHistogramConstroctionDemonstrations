package edu.tou;

import org.primefaces.model.chart.Axis;
import org.primefaces.model.chart.AxisType;
import org.primefaces.model.chart.BarChartModel;
import org.primefaces.model.chart.ChartSeries;

import javax.annotation.PostConstruct;
import javax.faces.bean.ManagedBean;
import java.io.Serializable;

/**
 * Created by kars on 7/18/16.
 */
@ManagedBean(name = "bean")
public class Bean implements Serializable {
    private BarChartModel model;

    public Bean() {
        model = new BarChartModel();
        ChartSeries boys = new ChartSeries();
        boys.setLabel("Boys");
        boys.set(100,100);
        boys.set(1700,100);
        boys.set(1457700,100);
        boys.set(145646700,100);
        boys.set(49545609845l,100);


        model.addSeries(boys);

        model.setTitle("Equi Depth Histogram");
        model.setLegendPosition("ne");

        Axis xAxis=model.getAxis(AxisType.X);
        xAxis.setLabel("Number Of Tuples");
        Axis yAxis=model.getAxis(AxisType.Y);
        yAxis.setLabel("N/B");
        yAxis.setTickFormat("-");
    }

    public BarChartModel getModel() {
        return model;
    }
}
