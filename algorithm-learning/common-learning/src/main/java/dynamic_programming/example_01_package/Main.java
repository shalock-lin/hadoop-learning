package dynamic_programming.example_01_package;

/**
 * @Author:linchonghui
 * @Date:5/5/2020
 * @Blog: https://github.com/Boomxiakalakaka/flink-learning
 */
public class Main {

    // 回溯算法实现。注意：我把输入的变量都定义成了成员变量
    private static int maxW = Integer.MIN_VALUE; // 结果放到maxW中
    private static int[] weight = {3, 2, 5, 5, 3};  // 物品重量
    private static int n = 5; // 物品个数
    private static int w = 9; // 背包承受的最大重量

    private static boolean[][] mem = new boolean[5][10]; // 备忘录，默认值false

    public static void main(String []args) {
        System.out.println(maxW);
        methodTwo(0, 0);
        System.out.println(maxW);
    }


    /**
     * 回溯算法
     *
     * 穷举所有可能性
     *
     * 时间复杂度 O(2^n)
     * 空间复杂度 O(1)
     *
     * 问题：由于穷举的过程中，存在大量的重复计算，如果能消除这些重复计算，则性能跟动态规划一样
     * 解决方案：备忘录模式
     */
    public static void methodOne(int i, int cw) {
        if (cw == w || i == n) {
            if (cw > maxW) maxW = cw;
            return;
        }
        methodOne(i+1, cw); //选择不装第i个物品
        if (cw + weight[i] <= w ) {
            methodOne(i+1, cw + weight[i]);
        }
    }

    /**
     * 回溯算法(备忘录模式)
     *
     * 记录重复计算的状态
     * 穷举所有可能性
     */
    public static void methodTwo(int i, int cw) {
        if (cw == w || i == n) {
            if (cw > maxW) maxW = cw;
            return;
        }
        if (mem[i][cw]) return;
        mem[i][cw] = true; //记录（i, cw）这个状态
        if (cw + weight[i] <= w ) {
            methodOne(i+1, cw + weight[i]);
        }
    }


}
