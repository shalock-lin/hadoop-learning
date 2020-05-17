package range_50_100.range_70;

/**
 * @Author:linchonghui
 * @Date:4/5/2020
 * @Blog: https://github.com/Boomxiakalakaka/flink-learning
 */

/**
 *
 假设你正在爬楼梯。需要 n 阶你才能到达楼顶。

 每次你可以爬 1 或 2 个台阶。你有多少种不同的方法可以爬到楼顶呢？

 注意：给定 n 是一个正整数。

 示例 1：

 输入： 2
 输出： 2
 解释： 有两种方法可以爬到楼顶。
 1.  1 阶 + 1 阶
 2.  2 阶
 示例 2：

 输入： 3
 输出： 3
 解释： 有三种方法可以爬到楼顶。
 1.  1 阶 + 1 阶 + 1 阶
 2.  1 阶 + 2 阶
 3.  2 阶 + 1 阶
 */
/**
 * 动态规划解题四步骤
 * 1. 定义子问题
 *    与原问题相似，但是规模较小
 *    子问题要具备：① 原问题能由子问题表示  ② 子问题的解可通过其他子问题的解来求出
 * 2. 写出子问题的递进关系
 *     列出子问题的推导公式
 * 3. 确定DP数组的计算顺序
 * 4. 空间优化(可选)
 *
 */
public class Main {

    public static void main(String []args) {
        System.out.println(methodTwo(4));
    }


    /**
     * 方法一：动态规划求解
     *
     * 时间复杂度（n）
     * 空间复杂度（n）
     */
    public static int methodOne(int n) {
        if (n<=0) return -1;
        if (n<=2) return n;

        int [] dpArrays = new int[n];
        dpArrays[0] = 1;
        dpArrays[1] = 2;
        for (int i=2; i< n; i++) {
            dpArrays[i] = dpArrays[i-1] + dpArrays[i-2];
        }
        return dpArrays[n-1];
    }

    /**
     * 方法二：动态规划求解（优化空间）
     * 不需要数组保存中间变量，因为只需要求最终数据即可
     *
     * 时间复杂度（n）
     * 空间复杂度（1）
     */
    public static int methodTwo(int n) {
        if (n<=0) return -1;
        if (n<=2) return n;

        int one = 1;
        int two = 2;
        int three = one + two;
        for (int i=2; i< n; i++) {
            three = one+two;
            one = two;
            two = three;
        }
        return three;
    }
}
