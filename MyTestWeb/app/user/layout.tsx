import type { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'Agent Desk · 专属智能客服',
  description: '面向终端用户的租户专属 Agent 对话页面。',
};

export default function UserLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return children;
}
