package org.reactiveminds.txpipe.utils;

public class Helper {

	private Helper() {
	}
	/**
	 * Replace the first substring within the first start and end tag with the given replacement.
	 * @param in input string
	 * @param rep replacement string
	 * @param sTag start tag
	 * @param eTag end tag
	 * @return
	 */
	public static String replaceWithinTags(String in, String rep, char sTag, char eTag) {
		int i = in.indexOf(sTag);
		if(i == -1)
			return in;
		int j = in.indexOf(eTag);
		if(j == -1)
			return in;
		StringBuilder s = new StringBuilder();
		s.append(in.substring(0, i));
		s.append(rep);
		s.append(in.substring(j+1));
		
		return s.toString();
	}
	/**
	 * Extracts the first substring within the first start and end tag.
	 *@param in input string
	 * @param sTag start tag
	 * @param eTag end tag
	 * @return
	 */
	public static String extractWithinTags(String in, char sTag, char eTag) {
		int i = in.indexOf(sTag);
		if(i == -1)
			return "";
		int j = in.indexOf(eTag);
		if(j == -1)
			return "";
		
		return in.substring(i+1, j);
	}
	
	public static void main(String[] args) {
		System.out.println(extractWithinTags("expecting {catch} here", '{', '}'));
		System.out.println(extractWithinTags("expecting  here", '{', '}'));
		System.out.println(extractWithinTags("expecting catch} here", '{', '}'));
		System.out.println(extractWithinTags("expecting {catch here", '{', '}'));
		System.out.println(extractWithinTags("expecting {cat}ch} here", '{', '}'));
		System.out.println(extractWithinTags("expec{ting {cat}ch} here", '{', '}'));
		System.out.println(extractWithinTags("expec{ting} {cat}ch} here", '{', '}'));
		
		System.out.println(replaceWithinTags("expecting {catch} here", "xxx", '{', '}'));
		System.out.println(replaceWithinTags("expecting  here", "xxx",  '{', '}'));
		System.out.println(replaceWithinTags("expecting catch} here", "xxx",  '{', '}'));
		System.out.println(replaceWithinTags("expecting {catch here", "xxx",  '{', '}'));
		System.out.println(replaceWithinTags("expecting {cat}ch} here", "xxx",  '{', '}'));
		System.out.println(replaceWithinTags("expec{ting {cat}ch} here", "xxx",  '{', '}'));
		System.out.println(replaceWithinTags("expec{ting} {cat}ch} here", "xxx",  '{', '}'));
	}
}
