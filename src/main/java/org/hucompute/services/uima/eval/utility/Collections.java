package org.hucompute.services.uima.eval.utility;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class Collections
{
	protected static Random random = new Random();

	/**
	 * Chooses a random subset of size `count` from the given set.
	 * If `count` is bigger than the set's size, the whole set is returned.
	 *
	 * Currently uses a HashSet, probably should be somehow modified to allow
	 * parameterization.
	 */
	public static <E> Set<E> chooseSubset(Set<E> set, int count)
	{
		int n = set.size();
		if (count > n)
		{
			return set;
		}

		Set<E> result = new HashSet<>();
		E[] elements = (E[]) set.toArray();
		for (int i = 0; i < count; i++)
		{
			int k = random.nextInt(n);
			n--;
			E tmp = elements[n];
			elements[n] = elements[k];
			elements[k] = tmp;
			result.add(elements[n]);
		}
		return result;
	}
}
