import type { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'AgentOps · 管理员验证台',
  description: '多租户 Agent 管理与实战验证台。',
};

export default function AdminLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return children;
}
