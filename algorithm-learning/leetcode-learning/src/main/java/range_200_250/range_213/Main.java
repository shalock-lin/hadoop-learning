package range_200_250.range_213;

/**
 * @Author:linchonghui
 * @Date:4/5/2020
 * @Blog: https://github.com/Boomxiakalakaka/flink-learning
 */

/**
 * 你是一个专业的小偷，计划偷窃沿街的房屋，每间房内都藏有一定的现金。这个地方所有的房屋都围成一圈，这意味着第一个房屋和最后一个房屋是紧挨着的。同时，相邻的房屋装有相互连通的防盗系统，如果两间相邻的房屋在同一晚上被小偷闯入，系统会自动报警。
 *
 * 给定一个代表每个房屋存放金额的非负整数数组，计算你在不触动警报装置的情况下，能够偷窃到的最高金额。
 *
 * 示例 1:
 *
 * 输入: [2,3,2]
 * 输出: 3
 * 解释: 你不能先偷窃 1 号房屋（金额 = 2），然后偷窃 3 号房屋（金额 = 2）, 因为他们是相邻的。
 * 示例 2:
 *
 * 输入: [1,2,3,1]
 * 输出: 4
 * 解释: 你可以先偷窃 1 号房屋（金额 = 1），然后偷窃 3 号房屋（金额 = 3）。
 *      偷窃到的最高金额 = 1 + 3 = 4 。
 *
 * 来源：力扣（LeetCode）
 * 链接：https://leetcode-cn.com/problems/house-robber-ii
 * 著作权归领扣网络所有。商业转载请联系官方授权，非商业转载请注明出处。
 */
public class Main {

    public static void main(String []args) {
        int []nums = {1,2,1,1};
        System.out.println(methodOne(nums));
    }

    /**
     * 动态规划求解
     *
     环状排列意味着第一个房子和最后一个房子中只能选择一个偷窃，
     因此可以把此环状排列房间问题约化为两个单排排列房间子问题：

     在不偷窃第一个房子的情况下，最大金额是 1p
     在不偷窃最后一个房子的情况下，最大金额是2p

     综合偷窃最大金额： 为以上两种情况的较大值，即 max(p1,p2)
     *
     * 时间复杂度 O(n)
     * 空间复杂度 O(n)
     */
    public static int methodOne(int[] nums) {
        if (nums.length <= 0) return 0;
        if (nums.length == 1) return nums[0];
        if (nums.length == 2) return Math.max(nums[0],nums[1]);

        int p1 = 0,p2 = 0;

        //statisk p1
        int N = nums.length;
        int []dp = new int[N+1];
        dp[0] = 0;
        dp[1] = nums[1];
        for (int i=3; i<= N; i++) {
            dp[i] = Math.max(dp[i-1], dp[i-2]+nums[i-1]);
        }
        p1 = dp[N];

        //statisk p2
        dp = new int[N+1];
        dp[0] = 0;
        dp[1] = nums[0];
        for (int i=2; i< N; i++) {
            dp[i] = Math.max(dp[i-1], dp[i-2]+nums[i-1]);
        }
        p2 = dp[N];

        return Math.max(p1,p2);
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
        if (nums.length == 2) return Math.max(nums[0],nums[1]);

        int N = nums.length;
        int one =0;
        int two = nums[0];
        int three = Math.max(one, two);
        for (int i=2; i<= N; i++) {
            three = Math.max(one+nums[i-1], two);
            one = two;
            two = three;
        }
        three = Math.max(one+nums[N-1]-nums[0], two);
        return three;
    }
}
