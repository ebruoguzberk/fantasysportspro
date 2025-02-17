import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from datetime import datetime, timedelta

class DashboardVisualizer:
    @staticmethod
    def create_player_performance_chart(player_stats):
        """Creates a line chart showing player performance over time"""
        df = pd.DataFrame(player_stats)
        if df.empty:
            return go.Figure()

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df['week'],
            y=df['points'],
            mode='lines+markers',
            name='Points',
            line=dict(color='#00ff9f', width=2),
            marker=dict(size=8, symbol='circle')
        ))

        fig.update_layout(
            title='Player Performance Over Time',
            template='plotly_dark',
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            xaxis_title='Week',
            yaxis_title='Points',
            hovermode='x unified'
        )
        return fig

    @staticmethod
    def create_animated_stat_chart(player_stats, stat_name):
        """Creates an animated chart for a specific stat"""
        df = pd.DataFrame(player_stats)
        df = df.sort_values('week')

        frames = []
        for week in df['week'].unique():
            frame_data = df[df['week'] <= week]
            frames.append(go.Frame(
                data=[go.Scatter(
                    x=frame_data['week'],
                    y=frame_data[stat_name],
                    mode='lines+markers',
                    line=dict(color='#00ff9f', width=2),
                    marker=dict(size=8)
                )],
                name=f'Week {week}'
            ))

        fig = go.Figure(
            data=[go.Scatter(
                x=[df['week'].iloc[0]],
                y=[df[stat_name].iloc[0]],
                mode='lines+markers',
                line=dict(color='#00ff9f', width=2),
                marker=dict(size=8)
            )],
            frames=frames
        )

        fig.update_layout(
            title=f'{stat_name.replace("_", " ").title()} Progress',
            template='plotly_dark',
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            xaxis_title='Week',
            yaxis_title=stat_name.replace('_', ' ').title(),
            updatemenus=[dict(
                type='buttons',
                showactive=False,
                buttons=[
                    dict(label='Play',
                         method='animate',
                         args=[None, {'frame': {'duration': 500, 'redraw': True},
                                    'fromcurrent': True,
                                    'transition': {'duration': 300}}]),
                    dict(label='Pause',
                         method='animate',
                         args=[[None], {'frame': {'duration': 0, 'redraw': False},
                                      'mode': 'immediate',
                                      'transition': {'duration': 0}}])
                ]
            )],
            sliders=[{
                'currentvalue': {'prefix': 'Week: '},
                'steps': [{'args': [[f.name], {'frame': {'duration': 0, 'redraw': False},
                                                 'mode': 'immediate',
                                                 'transition': {'duration': 0}}],
                          'label': str(k),
                          'method': 'animate'}
                         for k, f in enumerate(frames)]
            }]
        )
        return fig

    @staticmethod
    def create_real_time_performance_indicator(current_stats, previous_stats, stat_name):
        """Creates a real-time performance indicator comparing current vs previous stats"""
        if not current_stats or not previous_stats:
            return None

        current_value = current_stats.get(stat_name, 0)
        previous_value = previous_stats.get(stat_name, 0)

        delta = current_value - previous_value
        delta_color = '#00ff9f' if delta >= 0 else '#ff4b4b'

        fig = go.Figure()
        fig.add_trace(go.Indicator(
            mode="number+delta",
            value=current_value,
            delta={'reference': previous_value,
                   'relative': True,
                   'valueformat': '.1%'},
            title={'text': stat_name.replace('_', ' ').title()},
            domain={'y': [0, 1], 'x': [0, 1]}
        ))

        fig.update_layout(
            template='plotly_dark',
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            height=200
        )
        return fig

    @staticmethod
    def create_position_distribution_chart(players_data):
        """Creates a pie chart showing points distribution by position"""
        df = pd.DataFrame(players_data)
        if df.empty:
            return go.Figure()

        fig = px.pie(
            df,
            names='position',
            values='points',
            title='Points Distribution by Position',
            color_discrete_sequence=px.colors.sequential.Plasma
        )

        fig.update_layout(
            template='plotly_dark',
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        return fig

    @staticmethod
    def create_leaderboard_chart(leaderboard_data):
        """Creates a bar chart showing top performers"""
        if not leaderboard_data:
            return go.Figure()

        df = pd.DataFrame(leaderboard_data)
        # Convert numeric columns from string to float
        numeric_cols = ['passing_yards', 'rushing_yards', 'receiving_yards', 'touchdowns', 'points']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df['name'] if 'name' in df.columns else df.index,
            y=df['points'] if 'points' in df.columns else [0] * len(df),
            marker_color='#00ff9f',
            text=df['points'] if 'points' in df.columns else [0] * len(df),
            textposition='auto',
        ))

        fig.update_layout(
            title='Top Performers',
            template='plotly_dark',
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            xaxis_title='Player',
            yaxis_title='Points',
            showlegend=False
        )
        return fig