package range_150_200.range_198;

/**
 * @Author:linchonghui
 * @Date:4/5/2020
 * @Blog: https://github.com/Boomxiakalakaka/flink-learning
 */

/**
 *
 你是一个专业的小偷，计划偷窃沿街的房屋。每间房内都藏有一定的现金，影响你偷窃的唯一制约因素就是相邻的房屋装有相互连通的防盗系统，如果两间相邻的房屋在同一晚上被小偷闯入，系统会自动报警。

 给定一个代表每个房屋存放金额的非负整数数组，计算你在不触动警报装置的情况下，能够偷窃到的最高金额。

 示例 1:

 输入: [1,2,3,1]
 输出: 4
 解释: 偷窃 1 号房屋 (金额 = 1) ，然后偷窃 3 号房屋 (金额 = 3)。
 偷窃到的最高金额 = 1 + 3 = 4 。
 示例 2:

 输入: [2,7,9,3,1]
 输出: 12
 解释: 偷窃 1 号房屋 (金额 = 2), 偷窃 3 号房屋 (金额 = 9)，接着偷窃 5 号房屋 (金额 = 1)。
 偷窃到的最高金额 = 2 + 9 + 1 = 12 。
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
 */
public class Main {

    public static void main(String []args) {
        int []nums = {2,7,9,3,1};
        System.out.println(methodTwo(nums));
    }

    /**
     * 动态规划求解
     *
     * 时间复杂度 O(n)
     * 空间复杂度 O(n)
     */
    public static int methodOne(int[] nums) {
        if (nums.length <= 0) return 0;
        if (nums.length == 1) return nums[0];

        int N = nums.length;
        int []dp = new int[N+1];
        dp[0] = 0;
        dp[1] = nums[0];
        for (int i=2; i<= N; i++) {
            dp[i] = Math.max(dp[i-1], dp[i-2]+nums[i-1]);
        }
        return dp[N];
    }

    /**
     * 动态规划求解(优化版)
     *
     * 时间复杂度 O(n)
     * 空间复杂度 O(1)
     */
    public static int methodTwo(int[] nums) {
        if (nums.length <= 0) return 0;
        if (nums.length == 1) return nums[0];

        int N = nums.length;
        int one =0;
        int two = nums[0];
        int three = Math.max(one, two);
        for (int i=2; i<= N; i++) {
            three = Math.max(one+nums[i-1], two);
            one = two;
            two = three;
        }
        return three;
    }
}
