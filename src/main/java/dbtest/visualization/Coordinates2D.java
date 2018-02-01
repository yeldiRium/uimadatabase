package dbtest.visualization;

public class Coordinates2D<T extends Number> {
	T x;
	T y;

	public Coordinates2D(T x, T y)
	{
		this.x = x;
		this.y = y;
	}

	public T getX()
	{
		return x;
	}

	public T getY()
	{
		return y;
	}
}
