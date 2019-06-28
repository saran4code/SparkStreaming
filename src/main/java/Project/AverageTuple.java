package Project;
import java.io.Serializable;
public class AverageTuple implements Serializable{
	private static final long serialVersionUID = 332323;
	
	private int count;
	private double averageOpen;
	private double averageClose;
	private double maxprofit;
	
	public AverageTuple() {
		super();
		this.count = 0;
		this.averageOpen = 0;
		this.averageClose = 0;
		this.maxprofit = 0;
	}
	
	public AverageTuple(int count, double averageOpen, double averageClose, double maxprofit) {
		super();
		this.count = count;
		this.averageOpen = averageOpen;
		this.averageClose = averageClose;
		this.maxprofit = maxprofit;
	}
	
	@Override
	public String toString() {
		Double avgOpen=(averageOpen/count);
		Double avgClose=(averageClose/count);
		
		return "AvgOpen: " + avgOpen+ "AvgClose:" + avgClose+ "MaxProfit:" + maxprofit+ "Count: " + count;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public double getAverageOpen() {
		return averageOpen;
	}

	public void setAverageOpen(double averageOpen) {
		this.averageOpen = averageOpen;
	}

	public double getAverageClose() {
		return averageClose;
	}

	public void setAverageClose(double averageClose) {
		this.averageClose = averageClose;
	}

	public double getMaxprofit() {
		return maxprofit;
	}

	public void setMaxprofit(double averageClose, double averageOpen, int count) {
		Double avgOpen=(averageOpen/count);
		Double avgClose=(averageClose/count);
		this.maxprofit = avgClose-avgOpen;
	}
	
	
	
	}

